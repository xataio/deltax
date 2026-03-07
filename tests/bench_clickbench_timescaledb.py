"""ClickBench benchmark for TimescaleDB (TSL and OSS variants).

Compares TimescaleDB compression performance against pg_seaturtle on the same
ClickBench dataset and queries.

TSL variant (full license): uncompressed hypertable + two compression configs
  - "matching": segment_by=CounterID, order_by=EventTime (same as pg_seaturtle)
  - "default": no segment_by, order_by=EventTime only
OSS variant (Apache 2): uncompressed hypertable only (no compression support)

Controlled by TSDB_VARIANT env var: "tsl" (default) or "oss".

Run with:
    TSDB_VARIANT=tsl pytest tests/bench_clickbench_timescaledb.py -v -s
    TSDB_VARIANT=oss pytest tests/bench_clickbench_timescaledb.py -v -s
"""

import os
import subprocess
import time
import uuid

import psycopg
import pytest

from clickbench_data import (
    CREATE_TABLE_SQL,
    NUM_FILES,
    TIMED_RUNS,
    WARMUP_RUNS,
    load_parquet_files,
    query_results_to_dict,
    run_queries,
    save_bench_results,
)
from clickbench_queries import QUERIES

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

TSDB_VARIANT = os.environ.get("TSDB_VARIANT", "tsl").lower()

VARIANT_CONFIG = {
    "tsl": {
        "image": "timescale/timescaledb:latest-pg17",
        "container": "tsdb_tsl_bench",
        "port": 15433,
    },
    "oss": {
        "image": "timescale/timescaledb:latest-pg17-oss",
        "container": "tsdb_oss_bench",
        "port": 15434,
    },
}

PG_USER = "postgres"
PG_PASSWORD = "postgres"


# ---------------------------------------------------------------------------
# Docker container management
# ---------------------------------------------------------------------------

def _start_container(config):
    """Start a TimescaleDB Docker container."""
    container = config["container"]
    image = config["image"]
    port = config["port"]

    # Clean up any leftover container
    subprocess.run(["docker", "rm", "-f", container], capture_output=True)

    subprocess.check_call([
        "docker", "run", "-d",
        "--name", container,
        "-p", f"{port}:5432",
        "-e", f"POSTGRES_PASSWORD={PG_PASSWORD}",
        image,
    ])

    # Wait for readiness
    deadline = time.time() + 60
    while time.time() < deadline:
        result = subprocess.run(
            ["docker", "exec", container, "pg_isready", "-U", PG_USER],
            capture_output=True,
        )
        if result.returncode == 0:
            return
        time.sleep(1)
    raise TimeoutError(f"TimescaleDB ({container}) not ready after 60s")


def _stop_container(config):
    """Stop and remove the Docker container."""
    subprocess.run(["docker", "rm", "-f", config["container"]], capture_output=True)


def _connect(config, dbname="postgres", autocommit=False):
    """Return a psycopg connection to the TimescaleDB instance."""
    return psycopg.connect(
        host="localhost",
        port=config["port"],
        user=PG_USER,
        password=PG_PASSWORD,
        dbname=dbname,
        autocommit=autocommit,
    )


# ---------------------------------------------------------------------------
# TimescaleDB setup
# ---------------------------------------------------------------------------

def setup_timescaledb(conn):
    """Create extension, table, load data, convert to hypertable."""
    conn.execute("CREATE EXTENSION IF NOT EXISTS timescaledb")
    conn.commit()

    # Create plain table first (bulk load is faster on plain tables)
    conn.execute(CREATE_TABLE_SQL)
    conn.commit()

    print(f"\n--- Loading {NUM_FILES} ClickBench parquet file(s) ---")
    load_parquet_files(conn, NUM_FILES)

    row_count = conn.execute("SELECT count(*) FROM hits").fetchone()[0]
    print(f"  Total rows loaded: {row_count:,}")

    # Convert to hypertable with migrate_data
    print("\n--- Converting to hypertable ---")
    t0 = time.monotonic()
    conn.execute(
        "SELECT create_hypertable('hits', 'eventtime', "
        "chunk_time_interval => INTERVAL '1 day', migrate_data => true)"
    )
    conn.commit()
    elapsed = time.monotonic() - t0
    print(f"  Hypertable created in {elapsed:.1f}s")

    # Show chunk info
    chunks = conn.execute(
        "SELECT count(*) FROM show_chunks('hits')"
    ).fetchone()[0]
    print(f"  Chunks: {chunks}")

    return row_count


# ---------------------------------------------------------------------------
# Compression operations (TSL only)
# ---------------------------------------------------------------------------

def compress_matching(conn):
    """Compress with segment_by=CounterID, order_by=EventTime (matching pg_seaturtle).

    Returns elapsed time in seconds.
    """
    print("\n--- Compressing (matching config: segment_by=CounterID) ---")
    t0 = time.monotonic()
    conn.execute(
        "ALTER TABLE hits SET ("
        "timescaledb.compress, "
        "timescaledb.compress_segmentby = 'counterid', "
        "timescaledb.compress_orderby = 'eventtime')"
    )
    conn.commit()

    conn.execute("SELECT compress_chunk(ch) FROM show_chunks('hits') ch")
    conn.commit()
    elapsed = time.monotonic() - t0
    print(f"  Compression completed in {elapsed:.1f}s")
    return elapsed


def decompress_all(conn):
    """Decompress all chunks."""
    print("\n--- Decompressing all chunks ---")
    t0 = time.monotonic()
    conn.execute("SELECT decompress_chunk(ch) FROM show_chunks('hits') ch")
    conn.commit()
    elapsed = time.monotonic() - t0
    print(f"  Decompression completed in {elapsed:.1f}s")


def compress_default(conn):
    """Compress with no segment_by, order_by=EventTime only.

    Returns elapsed time in seconds.
    """
    print("\n--- Compressing (default config: no segment_by) ---")
    t0 = time.monotonic()
    conn.execute(
        "ALTER TABLE hits SET ("
        "timescaledb.compress, "
        "timescaledb.compress_segmentby = '', "
        "timescaledb.compress_orderby = 'eventtime')"
    )
    conn.commit()

    conn.execute("SELECT compress_chunk(ch) FROM show_chunks('hits') ch")
    conn.commit()
    elapsed = time.monotonic() - t0
    print(f"  Compression completed in {elapsed:.1f}s")
    return elapsed


def get_hypertable_size(conn):
    """Get total hypertable disk size in bytes (works for both TSL and OSS)."""
    row = conn.execute(
        "SELECT hypertable_size('hits')"
    ).fetchone()
    return int(row[0] or 0)


def get_compression_stats(conn):
    """Get compression stats from TimescaleDB.

    Returns (total_before_bytes, total_after_bytes).
    """
    rows = conn.execute(
        "SELECT "
        "  sum(before_compression_total_bytes) AS before_bytes, "
        "  sum(after_compression_total_bytes) AS after_bytes "
        "FROM chunk_compression_stats('hits')"
    ).fetchone()
    return (int(rows[0] or 0), int(rows[1] or 0))


def print_disk_size(label, size_bytes):
    """Print disk size for a hypertable."""
    print(f"\n### Disk Size ({label})")
    print(f"  Total: {size_bytes / 1e6:.1f} MB")


def print_compression_stats_tsdb(label, before_bytes, after_bytes):
    """Print compression stats for a TimescaleDB config."""
    before_mb = before_bytes / 1e6
    after_mb = after_bytes / 1e6
    ratio = before_bytes / after_bytes if after_bytes > 0 else 0
    print(f"\n### Compression Stats ({label})")
    print(f"  Before: {before_mb:.1f} MB")
    print(f"  After:  {after_mb:.1f} MB")
    print(f"  Ratio:  {ratio:.1f}x")


# ---------------------------------------------------------------------------
# Result reporting
# ---------------------------------------------------------------------------

def print_tsl_results(uncompr, compr_match, compr_dflt, uncompr_size, match_stats, dflt_stats):
    """Print combined TSL results table."""
    match_before, match_after = match_stats
    dflt_before, dflt_after = dflt_stats

    print("\n### Query Performance (TimescaleDB TSL)")
    print()
    print(
        f"| {'Query':<6} | {'Description':<25} "
        f"| {'Uncompr (ms)':>13} "
        f"| {'Match (ms)':>11} "
        f"| {'Dflt (ms)':>10} "
        f"| {'Match Ratio':>12} "
        f"| {'Dflt Ratio':>11} |"
    )
    print(
        f"|{'-'*8}|{'-'*27}"
        f"|{'-'*15}"
        f"|{'-'*13}"
        f"|{'-'*12}"
        f"|{'-'*14}"
        f"|{'-'*13}|"
    )

    for qid, desc, _ in QUERIES:
        u = uncompr.get(qid, (float("inf"), None))[0]
        m = compr_match.get(qid, (float("inf"), None))[0]
        d = compr_dflt.get(qid, (float("inf"), None))[0]

        def _fmt(val):
            return f"{val:.1f}" if val != float("inf") else "ERR"

        def _ratio(base, comp):
            if base != float("inf") and comp != float("inf") and comp > 0:
                return f"{base / comp:.2f}x"
            return "N/A"

        print(
            f"| {qid:<6} | {desc:<25} "
            f"| {_fmt(u):>13} "
            f"| {_fmt(m):>11} "
            f"| {_fmt(d):>10} "
            f"| {_ratio(u, m):>12} "
            f"| {_ratio(u, d):>11} |"
        )

    print_disk_size("uncompressed hypertable", uncompr_size)
    print_compression_stats_tsdb("matching: segment_by=CounterID", match_before, match_after)
    print_compression_stats_tsdb("default: no segment_by", dflt_before, dflt_after)


def print_oss_results(uncompr):
    """Print OSS results table (uncompressed only)."""
    print("\n### Query Performance (TimescaleDB OSS — uncompressed hypertable)")
    print()
    print(f"| {'Query':<6} | {'Description':<25} | {'Time (ms)':>10} |")
    print(f"|{'-'*8}|{'-'*27}|{'-'*12}|")

    for qid, desc, _ in QUERIES:
        t = uncompr.get(qid, (float("inf"), None))[0]
        t_str = f"{t:.1f}" if t != float("inf") else "ERR"
        print(f"| {qid:<6} | {desc:<25} | {t_str:>10} |")


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def validate_results(label_a, results_a, label_b, results_b):
    """Validate that two result sets match. Returns list of mismatched query IDs."""
    mismatches = []
    for qid, desc, _ in QUERIES:
        _, rows_a = results_a.get(qid, (None, None))
        _, rows_b = results_b.get(qid, (None, None))

        if rows_a is None or rows_b is None:
            print(f"  {qid}: SKIP (query errored in {label_a if rows_a is None else label_b})")
            continue

        if rows_a == rows_b:
            print(f"  {qid}: OK ({len(rows_a)} rows match)")
        else:
            mismatches.append(qid)
            print(f"  {qid}: MISMATCH ({label_a} vs {label_b})")
            print(f"    {label_a}: {len(rows_a)} rows, first={rows_a[:2]}")
            print(f"    {label_b}: {len(rows_b)} rows, first={rows_b[:2]}")

    return mismatches


# ---------------------------------------------------------------------------
# Pytest fixtures & tests
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def tsdb_container():
    """Start the TimescaleDB Docker container for the configured variant."""
    config = VARIANT_CONFIG[TSDB_VARIANT]
    print(f"\n--- Starting TimescaleDB ({TSDB_VARIANT}) container ---")
    print(f"  Image: {config['image']}")
    print(f"  Port:  {config['port']}")
    _start_container(config)

    yield config

    _stop_container(config)


@pytest.fixture(scope="class")
def tsdb_db(tsdb_container):
    """Create a database, load ClickBench data, create hypertable.

    Scoped to class so data is loaded once for all benchmark tests.
    """
    config = tsdb_container
    db_name = "bench_tsdb_" + uuid.uuid4().hex[:8]

    admin = _connect(config, autocommit=True)
    admin.execute(f'CREATE DATABASE "{db_name}"')
    admin.close()

    conn = _connect(config, dbname=db_name)
    setup_timescaledb(conn)

    yield conn

    conn.close()
    admin = _connect(config, autocommit=True)
    admin.execute(f'DROP DATABASE "{db_name}"')
    admin.close()


class TestClickBenchTimescaleDB:
    """ClickBench benchmark for TimescaleDB."""

    def test_benchmark(self, tsdb_db):
        conn = tsdb_db

        if TSDB_VARIANT == "tsl":
            self._run_tsl_benchmark(conn)
        else:
            self._run_oss_benchmark(conn)

    def _run_tsl_benchmark(self, conn):
        """TSL: uncompressed -> compress matching -> compress default."""
        # Phase 1: Uncompressed hypertable queries
        print("\n\n=== Phase 1: Uncompressed Hypertable Queries ===")
        uncompr_results = run_queries(conn, QUERIES, label="uncompr")

        # Capture uncompressed disk size
        uncompr_size = get_hypertable_size(conn)
        print(f"\n  Uncompressed hypertable size: {uncompr_size / 1e6:.1f} MB")

        # Phase 2: Compress with matching config
        print("\n=== Phase 2: Compress (matching pg_seaturtle config) ===")
        match_compress_time = compress_matching(conn)
        match_stats = get_compression_stats(conn)

        print("\n=== Phase 2b: Query compressed (matching) ===")
        compr_match_results = run_queries(conn, QUERIES, label="compr-match")

        # Phase 3: Decompress, then compress with default config
        print("\n=== Phase 3: Decompress ===")
        decompress_all(conn)

        print("\n=== Phase 3b: Compress (default config) ===")
        dflt_compress_time = compress_default(conn)
        dflt_stats = get_compression_stats(conn)

        print("\n=== Phase 3c: Query compressed (default) ===")
        compr_dflt_results = run_queries(conn, QUERIES, label="compr-dflt")

        # Phase 4: Validate results
        print("\n=== Phase 4: Validating Results ===")
        all_mismatches = []

        print("\n  --- uncompr vs compr-match ---")
        all_mismatches.extend(
            validate_results("uncompr", uncompr_results, "compr-match", compr_match_results)
        )

        print("\n  --- uncompr vs compr-dflt ---")
        all_mismatches.extend(
            validate_results("uncompr", uncompr_results, "compr-dflt", compr_dflt_results)
        )

        # Phase 5: Print combined report
        print("\n\n" + "=" * 72)
        print("  ClickBench Benchmark Results (TimescaleDB TSL)")
        print(f"  Files: {NUM_FILES}, Warmup: {WARMUP_RUNS}, Timed runs: {TIMED_RUNS}")
        print("=" * 72)

        print_tsl_results(
            uncompr_results, compr_match_results, compr_dflt_results,
            uncompr_size, match_stats, dflt_stats,
        )

        # Save results for cross-system comparison
        match_before, match_after = match_stats
        dflt_before, dflt_after = dflt_stats
        save_bench_results("timescaledb_tsl", {
            "uncompressed_queries": query_results_to_dict(uncompr_results),
            "compressed_matching_queries": query_results_to_dict(compr_match_results),
            "compressed_default_queries": query_results_to_dict(compr_dflt_results),
            "raw_bytes": match_before,
            "compressed_matching_bytes": match_after,
            "compressed_default_bytes": dflt_after,
            "compression_ratio_matching": match_before / match_after if match_after > 0 else 0,
            "compression_ratio_default": dflt_before / dflt_after if dflt_after > 0 else 0,
            "compression_time_matching_s": match_compress_time,
            "compression_time_default_s": dflt_compress_time,
        })

        assert not all_mismatches, (
            f"Result mismatch for queries: {all_mismatches}. "
            "Compressed query results differ from uncompressed."
        )

    def _run_oss_benchmark(self, conn):
        """OSS: uncompressed hypertable only."""
        print("\n\n=== Uncompressed Hypertable Queries (OSS) ===")
        uncompr_results = run_queries(conn, QUERIES, label="uncompr")

        # Capture disk size
        hypertable_size = get_hypertable_size(conn)

        # Print report
        print("\n\n" + "=" * 72)
        print("  ClickBench Benchmark Results (TimescaleDB OSS)")
        print(f"  Files: {NUM_FILES}, Warmup: {WARMUP_RUNS}, Timed runs: {TIMED_RUNS}")
        print("=" * 72)

        print_oss_results(uncompr_results)
        print_disk_size("uncompressed hypertable", hypertable_size)

        # Save results for cross-system comparison
        save_bench_results("timescaledb_oss", {
            "uncompressed_queries": query_results_to_dict(uncompr_results),
            "raw_bytes": hypertable_size,
        })
