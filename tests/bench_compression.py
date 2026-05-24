"""Benchmark suite for Phase 2 compression.

Run with: pytest tests/bench_compression.py -v -s
(Requires PG_DELTAX_IMAGE env var)
"""

import time
import pytest


def setup_bench_table(conn, table_name="bench_metrics", n_devices=1000, n_points=10000):
    """Create table and insert n_devices * n_points rows of IoT data."""
    # Pin "now" so partitions cover our test timestamps
    conn.execute("SET pg_deltax.mock_now = '2025-01-15 12:00:00+00'")
    conn.execute(f"""
        CREATE TABLE {table_name} (
            ts TIMESTAMPTZ NOT NULL,
            device_id TEXT NOT NULL,
            temperature DOUBLE PRECISION,
            pressure DOUBLE PRECISION,
            status BOOLEAN
        )
    """)
    conn.execute(f"""
        SELECT deltax.deltax_create_table('{table_name}', 'ts', '1 day'::interval)
    """)
    conn.commit()

    # Generate data using generate_series for speed
    conn.execute(f"""
        INSERT INTO {table_name} (ts, device_id, temperature, pressure, status)
        SELECT
            '2025-01-15 00:00:00+00'::timestamptz + (p * interval '1 second'),
            'device-' || lpad((d % {n_devices})::text, 4, '0'),
            20.0 + (d % {n_devices}) * 0.5 + sin(p::float / 100) * 5,
            1013.0 + (d % {n_devices}) * 0.1 + cos(p::float / 100) * 2,
            (p % 3) != 0
        FROM generate_series(0, {n_points - 1}) AS p,
             generate_series(0, {n_devices - 1}) AS d
    """)
    conn.commit()


class TestBenchCompression:
    """Benchmarks for compression operations.

    These tests are marked with a longer timeout and print results.
    They use smaller data sizes for CI; increase for production benchmarks.
    """

    @pytest.fixture()
    def bench_db(self, db):
        """Set up a database with a large test table."""
        # Use smaller sizes for CI (10 devices × 1000 points = 10K rows)
        setup_bench_table(db, n_devices=10, n_points=1000)
        db.execute(
            "SELECT deltax.deltax_enable_compression('bench_metrics', "
            "segment_by => ARRAY['device_id'], "
            "order_by => ARRAY['ts'])"
        )
        db.commit()
        yield db

    def _get_partition(self, conn, ts="2025-01-15"):
        """Get a partition name containing the given timestamp."""
        parts = conn.execute(
            "SELECT partition_name FROM deltax.deltax_partition_info('bench_metrics') "
            f"WHERE range_start <= '{ts}'::timestamptz "
            f"AND range_end > '{ts}'::timestamptz"
        ).fetchall()
        assert len(parts) > 0
        return parts[0][0]

    def test_compression_ratio(self, bench_db):
        """Measure compression ratio."""
        part_name = self._get_partition(bench_db)

        row_count = bench_db.execute(
            f"SELECT count(*) FROM \"{part_name}\""
        ).fetchone()[0]

        raw_size = bench_db.execute(
            f"SELECT pg_total_relation_size('\"{part_name}\"'::regclass)"
        ).fetchone()[0]

        result = bench_db.execute(
            f"SELECT deltax.deltax_compress_partition('{part_name}')"
        ).fetchone()[0]
        bench_db.commit()

        stats = bench_db.execute(
            "SELECT raw_size, compressed_size, compression_ratio "
            f"FROM deltax.deltax_compression_stats('bench_metrics') "
            f"WHERE partition_name = '{part_name}'"
        ).fetchone()

        print(f"\n--- Compression Ratio ---")
        print(f"Rows:       {row_count}")
        print(f"Raw size:   {stats[0]:,} bytes")
        print(f"Compressed: {stats[1]:,} bytes")
        print(f"Ratio:      {stats[2]:.1f}x")

    def test_compression_throughput(self, bench_db):
        """Measure compress + decompress throughput."""
        part_name = self._get_partition(bench_db)

        row_count = bench_db.execute(
            f"SELECT count(*) FROM \"{part_name}\""
        ).fetchone()[0]

        # Compress
        start = time.monotonic()
        bench_db.execute(f"SELECT deltax.deltax_compress_partition('{part_name}')")
        bench_db.commit()
        compress_time = time.monotonic() - start

        # Decompress
        start = time.monotonic()
        bench_db.execute(f"SELECT deltax.deltax_decompress_partition('{part_name}')")
        bench_db.commit()
        decompress_time = time.monotonic() - start

        print(f"\n--- Throughput ---")
        print(f"Rows:             {row_count}")
        print(f"Compress time:    {compress_time:.3f}s ({row_count / compress_time:.0f} rows/s)")
        print(f"Decompress time:  {decompress_time:.3f}s ({row_count / decompress_time:.0f} rows/s)")

    def test_query_latency_point(self, bench_db):
        """Measure point query latency: compressed vs uncompressed."""
        part_name = self._get_partition(bench_db)

        # Uncompressed query
        start = time.monotonic()
        bench_db.execute(
            f"SELECT avg(temperature) FROM \"{part_name}\" "
            "WHERE device_id = 'device-0005' "
            "AND ts BETWEEN '2025-01-15 00:00:00+00' AND '2025-01-15 00:10:00+00'"
        ).fetchone()
        uncompressed_time = time.monotonic() - start

        # Compress
        bench_db.execute(f"SELECT deltax.deltax_compress_partition('{part_name}')")
        bench_db.commit()

        # Compressed query (goes through custom scan)
        start = time.monotonic()
        bench_db.execute(
            f"SELECT avg(temperature) FROM \"{part_name}\" "
            "WHERE device_id = 'device-0005' "
            "AND ts BETWEEN '2025-01-15 00:00:00+00' AND '2025-01-15 00:10:00+00'"
        ).fetchone()
        compressed_time = time.monotonic() - start

        print(f"\n--- Point Query Latency ---")
        print(f"Uncompressed: {uncompressed_time * 1000:.1f}ms")
        print(f"Compressed:   {compressed_time * 1000:.1f}ms")

    def test_query_latency_aggregation(self, bench_db):
        """Measure full aggregation latency."""
        part_name = self._get_partition(bench_db)

        start = time.monotonic()
        bench_db.execute(
            f"SELECT avg(temperature), min(temperature), max(temperature) "
            f"FROM \"{part_name}\" "
            "WHERE ts BETWEEN '2025-01-15 00:00:00+00' AND '2025-01-15 01:00:00+00'"
        ).fetchone()
        uncompressed_time = time.monotonic() - start

        bench_db.execute(f"SELECT deltax.deltax_compress_partition('{part_name}')")
        bench_db.commit()

        start = time.monotonic()
        bench_db.execute(
            f"SELECT avg(temperature), min(temperature), max(temperature) "
            f"FROM \"{part_name}\" "
            "WHERE ts BETWEEN '2025-01-15 00:00:00+00' AND '2025-01-15 01:00:00+00'"
        ).fetchone()
        compressed_time = time.monotonic() - start

        print(f"\n--- Aggregation Query Latency ---")
        print(f"Uncompressed: {uncompressed_time * 1000:.1f}ms")
        print(f"Compressed:   {compressed_time * 1000:.1f}ms")
