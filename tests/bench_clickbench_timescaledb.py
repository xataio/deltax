"""ClickBench benchmark for TimescaleDB.

Compares TimescaleDB compression performance against pg_deltax on the same
ClickBench dataset and queries. Uses the official ClickBench TimescaleDB config:
no segment_by, order_by=counterid,userid,eventtime.

Run with:
    pytest tests/bench_clickbench_timescaledb.py -v -s
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
from clickbench_queries import (
    LIMIT_TIE_QUERIES,
    NONDET_SORT_INFO,
    NONDETERMINISTIC_QUERIES,
    QUERIES,
    validate_nondet_query,
)

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
        "chunk_time_interval => INTERVAL '3 days', migrate_data => true)"
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

def compress_data(conn):
    """Compress matching official ClickBench TimescaleDB config.

    Returns elapsed time in seconds.
    """
    print("\n--- Compressing (orderby=counterid,userid,eventtime) ---")
    t0 = time.monotonic()
    conn.execute(
        "ALTER TABLE hits SET ("
        "timescaledb.compress, "
        "timescaledb.compress_segmentby = '', "
        "timescaledb.compress_orderby = 'counterid, userid, eventtime')"
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

def print_tsl_results(uncompr, compr, uncompr_size, compr_stats):
    """Print combined TSL results table."""
    compr_before, compr_after = compr_stats

    print("\n### Query Performance (TimescaleDB TSL)")
    print()
    print(
        f"| {'Query':<6} | {'Description':<25} "
        f"| {'Uncompr (ms)':>13} "
        f"| {'Compr (ms)':>11} "
        f"| {'Ratio':>6} |"
    )
    print(
        f"|{'-'*8}|{'-'*27}"
        f"|{'-'*15}"
        f"|{'-'*13}"
        f"|{'-'*8}|"
    )

    for qid, desc, _ in QUERIES:
        u = uncompr.get(qid, (float("inf"), None))[0]
        c = compr.get(qid, (float("inf"), None))[0]

        def _fmt(val):
            return f"{val:.1f}" if val != float("inf") else "ERR"

        def _ratio(base, comp):
            if base != float("inf") and comp != float("inf") and comp > 0:
                return f"{base / comp:.2f}x"
            return "N/A"

        print(
            f"| {qid:<6} | {desc:<25} "
            f"| {_fmt(u):>13} "
            f"| {_fmt(c):>11} "
            f"| {_ratio(u, c):>6} |"
        )

    print_disk_size("uncompressed hypertable", uncompr_size)
    print_compression_stats_tsdb("compressed", compr_before, compr_after)


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
    """Validate that two result sets match. Returns list of mismatched query IDs.

    Non-deterministic queries (ties in ORDER BY + LIMIT/OFFSET) are validated
    by row count only.  Deterministic queries use sorted comparison.
    """
    mismatches = []
    for qid, desc, _ in QUERIES:
        _, rows_a = results_a.get(qid, (None, None))
        _, rows_b = results_b.get(qid, (None, None))

        if rows_a is None or rows_b is None:
            print(f"  {qid}: SKIP (query errored in {label_a if rows_a is None else label_b})")
            continue

        if qid in NONDETERMINISTIC_QUERIES:
            if len(rows_a) != len(rows_b):
                mismatches.append(qid)
                print(f"  {qid}: MISMATCH row count ({label_a}={len(rows_a)}, {label_b}={len(rows_b)})")
            else:
                ok, detail = validate_nondet_query(
                    qid, rows_a, rows_b, NONDET_SORT_INFO.get(qid)
                )
                if ok:
                    print(f"  {qid}: OK ({detail})")
                else:
                    mismatches.append(qid)
                    print(f"  {qid}: MISMATCH ({detail})")
        elif qid in LIMIT_TIE_QUERIES:
            sk = LIMIT_TIE_QUERIES[qid]
            if len(rows_a) != len(rows_b):
                mismatches.append(qid)
                print(f"  {qid}: MISMATCH row count ({label_a}={len(rows_a)}, {label_b}={len(rows_b)})")
            elif len(rows_a) == 0:
                print(f"  {qid}: OK (0 rows)")
            else:
                a_tail, b_tail = rows_a[-1][sk], rows_b[-1][sk]
                a_head, b_head = rows_a[0][sk], rows_b[0][sk]
                a_stable = [r for r in rows_a if r[sk] != a_tail]
                b_stable = [r for r in rows_b if r[sk] != b_tail]
                a_stable = sorted([r for r in a_stable if r[sk] != a_head])
                b_stable = sorted([r for r in b_stable if r[sk] != b_head])
                if a_stable == b_stable:
                    n_tied = len(rows_a) - len(a_stable)
                    print(f"  {qid}: OK ({len(a_stable)} exact + {n_tied} tied rows)")
                else:
                    mismatches.append(qid)
                    print(f"  {qid}: MISMATCH (non-tied rows differ)!")
                    print(f"    {label_a} stable: {len(a_stable)} rows, first={a_stable[:2]}")
                    print(f"    {label_b} stable: {len(b_stable)} rows, first={b_stable[:2]}")
        elif sorted(rows_a) == sorted(rows_b):
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
        """TSL: uncompressed -> compress -> query compressed."""
        # Phase 1: Uncompressed hypertable queries
        print("\n\n=== Phase 1: Uncompressed Hypertable Queries ===")
        uncompr_results = run_queries(conn, QUERIES, label="uncompr")

        # Capture uncompressed disk size
        uncompr_size = get_hypertable_size(conn)
        print(f"\n  Uncompressed hypertable size: {uncompr_size / 1e6:.1f} MB")

        # Phase 2: Compress
        print("\n=== Phase 2: Compress ===")
        compress_time = compress_data(conn)
        compr_stats = get_compression_stats(conn)

        print("\n=== Phase 2b: Query compressed ===")
        compr_results = run_queries(conn, QUERIES, label="compr")

        # Phase 3: Validate results
        print("\n=== Phase 3: Validating Results ===")
        mismatches = validate_results("uncompr", uncompr_results, "compr", compr_results)

        # Phase 4: Print report
        print("\n\n" + "=" * 72)
        print("  ClickBench Benchmark Results (TimescaleDB TSL)")
        print(f"  Files: {NUM_FILES}, Warmup: {WARMUP_RUNS}, Timed runs: {TIMED_RUNS}")
        print("=" * 72)

        print_tsl_results(uncompr_results, compr_results, uncompr_size, compr_stats)

        # Save results for cross-system comparison
        compr_before, compr_after = compr_stats
        save_bench_results("timescaledb_tsl", {
            "uncompressed_queries": query_results_to_dict(uncompr_results),
            "compressed_queries": query_results_to_dict(compr_results),
            "raw_bytes": compr_before,
            "compressed_bytes": compr_after,
            "compression_ratio": compr_before / compr_after if compr_after > 0 else 0,
            "compression_time_s": compress_time,
        })

        assert not mismatches, (
            f"Result mismatch for queries: {mismatches}. "
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
