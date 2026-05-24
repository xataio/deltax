"""Integration tests for Parquet loading via COPY FROM with FORMAT deltax_compress."""

import os
import subprocess
import tempfile
from datetime import datetime, timezone, timedelta

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

CONTAINER_NAME = "pg_deltax_inttest"


def _docker_cp_to_container(local_path, container_path):
    """Copy a file into the running Docker container and make it readable."""
    cov_container = os.environ.get("PG_DELTAX_COV_CONTAINER", CONTAINER_NAME)
    subprocess.check_call(
        ["docker", "cp", local_path, f"{cov_container}:{container_path}"]
    )
    subprocess.check_call(
        ["docker", "exec", cov_container, "chmod", "644", container_path]
    )


def _docker_exec(cmd):
    """Run a command inside the container."""
    cov_container = os.environ.get("PG_DELTAX_COV_CONTAINER", CONTAINER_NAME)
    subprocess.check_call(["docker", "exec", cov_container] + cmd)


def _setup_table(db, table_name="pqtest", interval="3 days", segment_by=None):
    """Create a deltax table with compression enabled."""
    db.execute(
        f"CREATE TABLE {table_name} ("
        "ts TIMESTAMPTZ NOT NULL, device TEXT, value FLOAT8, count INT)"
    )
    db.commit()
    db.execute(f"SELECT deltax.deltax_create_table('{table_name}', 'ts', '{interval}')")
    db.commit()
    seg_by = f"ARRAY{segment_by}" if segment_by else "ARRAY[]::text[]"
    db.execute(
        f"SELECT deltax.deltax_enable_compression('{table_name}', "
        f"segment_by => {seg_by}, order_by => ARRAY['ts'])"
    )
    db.commit()


def _write_parquet(path, ts_list, devices, values, counts):
    """Write a parquet file with the given columns."""
    table = pa.table({
        "ts": pa.array(ts_list, type=pa.timestamp("us", tz="UTC")),
        "device": pa.array(devices, type=pa.string()),
        "value": pa.array(values, type=pa.float64()),
        "count": pa.array(counts, type=pa.int32()),
    })
    pq.write_table(table, path)


def test_parquet_basic_roundtrip(db):
    """Load a parquet file and verify all rows are queryable."""
    _setup_table(db)

    base = datetime.now(timezone.utc).replace(microsecond=0)
    n_rows = 100
    ts_list = [base + timedelta(seconds=i) for i in range(n_rows)]
    devices = [f"dev{i % 5}" for i in range(n_rows)]
    values = [float(i) * 1.5 for i in range(n_rows)]
    counts = [i for i in range(n_rows)]

    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
        _write_parquet(f.name, ts_list, devices, values, counts)
        local_path = f.name

    try:
        container_path = "/tmp/test_basic.parquet"
        _docker_cp_to_container(local_path, container_path)
        db.execute(
            f"COPY pqtest FROM '{container_path}' WITH (FORMAT deltax_compress)"
        )
        db.commit()

        result = db.execute("SELECT COUNT(*) FROM pqtest").fetchone()
        assert result[0] == n_rows

        # Verify values
        row = db.execute(
            "SELECT value, count FROM pqtest WHERE device = 'dev0' ORDER BY ts LIMIT 1"
        ).fetchone()
        assert row[0] == 0.0
        assert row[1] == 0

        # Verify aggregate
        total = db.execute("SELECT SUM(count) FROM pqtest").fetchone()
        assert total[0] == sum(range(n_rows))
    finally:
        os.unlink(local_path)


def test_parquet_multi_partition(db):
    """Parquet data spanning multiple partitions gets correctly routed."""
    _setup_table(db, interval="1 day")

    base = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(hours=12)
    n_rows = 300
    # Spread across ~2.5 days (300 rows × 12 min each)
    ts_list = [base + timedelta(minutes=i * 12) for i in range(n_rows)]
    devices = ["dev1"] * n_rows
    values = [1.0] * n_rows
    counts = [1] * n_rows

    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
        _write_parquet(f.name, ts_list, devices, values, counts)
        local_path = f.name

    try:
        container_path = "/tmp/test_multi.parquet"
        _docker_cp_to_container(local_path, container_path)
        db.execute(
            f"COPY pqtest FROM '{container_path}' WITH (FORMAT deltax_compress)"
        )
        db.commit()

        result = db.execute("SELECT COUNT(*) FROM pqtest").fetchone()
        assert result[0] == n_rows

        # Should have multiple compressed partitions
        stats = db.execute(
            "SELECT COUNT(*) FROM deltax.deltax_compression_stats('pqtest') "
            "WHERE is_compressed = true"
        ).fetchone()
        assert stats[0] >= 2
    finally:
        os.unlink(local_path)


def test_parquet_with_nulls(db):
    """Parquet files with NULL values load correctly."""
    _setup_table(db)

    base = datetime.now(timezone.utc).replace(microsecond=0)
    n_rows = 50
    ts_list = [base + timedelta(seconds=i) for i in range(n_rows)]
    devices = [None if i % 3 == 0 else f"dev{i}" for i in range(n_rows)]
    values = [None if i % 5 == 0 else float(i) for i in range(n_rows)]
    counts = [None if i % 7 == 0 else i for i in range(n_rows)]

    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
        _write_parquet(f.name, ts_list, devices, values, counts)
        local_path = f.name

    try:
        container_path = "/tmp/test_nulls.parquet"
        _docker_cp_to_container(local_path, container_path)
        db.execute(
            f"COPY pqtest FROM '{container_path}' WITH (FORMAT deltax_compress)"
        )
        db.commit()

        result = db.execute("SELECT COUNT(*) FROM pqtest").fetchone()
        assert result[0] == n_rows

        # Verify NULLs are preserved
        null_devices = db.execute(
            "SELECT COUNT(*) FROM pqtest WHERE device IS NULL"
        ).fetchone()
        expected_null_devices = sum(1 for i in range(n_rows) if i % 3 == 0)
        assert null_devices[0] == expected_null_devices
    finally:
        os.unlink(local_path)


def test_parquet_glob_multiple_files(db):
    """Load multiple parquet files via glob pattern."""
    _setup_table(db)

    base = datetime.now(timezone.utc).replace(microsecond=0)
    total_rows = 0

    # Create a directory in the container (readable by postgres)
    _docker_exec(["mkdir", "-p", "/tmp/pq_glob"])
    _docker_exec(["chmod", "755", "/tmp/pq_glob"])

    local_paths = []
    try:
        for file_idx in range(3):
            n_rows = 40 + file_idx * 10
            total_rows += n_rows
            offset = file_idx * 100
            ts_list = [base + timedelta(seconds=offset + i) for i in range(n_rows)]
            devices = [f"dev{file_idx}"] * n_rows
            values = [float(i) for i in range(n_rows)]
            counts = [i for i in range(n_rows)]

            with tempfile.NamedTemporaryFile(
                suffix=".parquet", prefix=f"part{file_idx}_", delete=False
            ) as f:
                _write_parquet(f.name, ts_list, devices, values, counts)
                local_paths.append(f.name)
                _docker_cp_to_container(f.name, f"/tmp/pq_glob/part{file_idx}.parquet")

        db.execute(
            "COPY pqtest FROM '/tmp/pq_glob/*.parquet' WITH (FORMAT deltax_compress)"
        )
        db.commit()

        result = db.execute("SELECT COUNT(*) FROM pqtest").fetchone()
        assert result[0] == total_rows

        # Verify each device loaded
        for i in range(3):
            cnt = db.execute(
                f"SELECT COUNT(*) FROM pqtest WHERE device = 'dev{i}'"
            ).fetchone()
            assert cnt[0] == 40 + i * 10
    finally:
        for p in local_paths:
            os.unlink(p)


def test_parquet_column_reorder(db):
    """Parquet columns in different order than PG table still load correctly."""
    _setup_table(db)

    base = datetime.now(timezone.utc).replace(microsecond=0)
    n_rows = 20

    # Write parquet with columns in different order than the PG table
    table = pa.table({
        "count": pa.array(list(range(n_rows)), type=pa.int32()),
        "value": pa.array([float(i) for i in range(n_rows)], type=pa.float64()),
        "device": pa.array(["reorder_dev"] * n_rows, type=pa.string()),
        "ts": pa.array(
            [base + timedelta(seconds=i) for i in range(n_rows)],
            type=pa.timestamp("us", tz="UTC"),
        ),
    })

    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
        pq.write_table(table, f.name)
        local_path = f.name

    try:
        container_path = "/tmp/test_reorder.parquet"
        _docker_cp_to_container(local_path, container_path)
        db.execute(
            f"COPY pqtest FROM '{container_path}' WITH (FORMAT deltax_compress)"
        )
        db.commit()

        result = db.execute("SELECT COUNT(*) FROM pqtest").fetchone()
        assert result[0] == n_rows

        row = db.execute(
            "SELECT device, value, count FROM pqtest ORDER BY ts LIMIT 1"
        ).fetchone()
        assert row[0] == "reorder_dev"
        assert row[1] == 0.0
        assert row[2] == 0
    finally:
        os.unlink(local_path)


def test_parquet_timestamp_millis(db):
    """Parquet files with millisecond timestamps are correctly converted."""
    _setup_table(db)

    base = datetime.now(timezone.utc).replace(microsecond=0)
    n_rows = 10

    # Write with millisecond precision
    table = pa.table({
        "ts": pa.array(
            [base + timedelta(seconds=i) for i in range(n_rows)],
            type=pa.timestamp("ms", tz="UTC"),
        ),
        "device": pa.array(["ms_dev"] * n_rows, type=pa.string()),
        "value": pa.array([1.0] * n_rows, type=pa.float64()),
        "count": pa.array([1] * n_rows, type=pa.int32()),
    })

    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
        pq.write_table(table, f.name)
        local_path = f.name

    try:
        container_path = "/tmp/test_ms.parquet"
        _docker_cp_to_container(local_path, container_path)
        db.execute(
            f"COPY pqtest FROM '{container_path}' WITH (FORMAT deltax_compress)"
        )
        db.commit()

        result = db.execute("SELECT COUNT(*) FROM pqtest").fetchone()
        assert result[0] == n_rows

        # Verify timestamp precision is correct
        row = db.execute("SELECT ts FROM pqtest ORDER BY ts LIMIT 1").fetchone()
        assert row[0] == base
    finally:
        os.unlink(local_path)
