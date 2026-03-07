"""ClickBench real-world benchmark for pg_seaturtle compression.

Uses the ClickBench dataset (Yandex Metrica web analytics, 100M rows, 107 columns)
to stress-test compression with realistic data.

Default: 1 parquet file (~1M rows, ~1GB in PG). Scale via CLICKBENCH_FILES=N env var.

Run with:
    PG_SEATURTLE_IMAGE=pg_seaturtle:pg17 pytest tests/bench_clickbench.py -v -s

Scale up:
    PG_SEATURTLE_IMAGE=pg_seaturtle:pg17 CLICKBENCH_FILES=5 pytest tests/bench_clickbench.py -v -s
"""

import os
import time

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
# Setup & compression
# ---------------------------------------------------------------------------

def setup_clickbench(conn, n_files: int):
    """Create the hits table, set up partitioning, and load data."""
    # Pin time to July 2013 so partitions cover the data range
    conn.execute("SET pg_seaturtle.mock_now = '2013-07-15 12:00:00+00'")
    conn.execute(CREATE_TABLE_SQL)
    conn.execute(
        "SELECT seaturtle_create_table('hits', 'eventtime', '1 day'::interval, 31)"
    )
    conn.commit()

    print(f"\n--- Loading {n_files} ClickBench parquet file(s) ---")
    load_parquet_files(conn, n_files)

    row_count = conn.execute("SELECT count(*) FROM hits").fetchone()[0]
    print(f"  Total rows loaded: {row_count:,}")
    return row_count


def enable_compression(conn):
    """Enable compression with segment_by=CounterID, order_by=EventTime."""
    conn.execute(
        "SELECT seaturtle_enable_compression('hits', "
        "segment_by => ARRAY['counterid'], "
        "order_by => ARRAY['eventtime'])"
    )
    conn.commit()
    print("  Compression enabled (segment_by=CounterID, order_by=EventTime)")


def compress_all_partitions(conn):
    """Compress all non-empty, non-default partitions. Returns per-partition stats."""
    partitions = conn.execute(
        "SELECT partition_name FROM seaturtle_partition_info('hits') "
        "WHERE partition_name NOT LIKE '%default%' "
        "ORDER BY partition_name"
    ).fetchall()

    results = []
    for (part_name,) in partitions:
        row_count = conn.execute(
            f'SELECT count(*) FROM "{part_name}"'
        ).fetchone()[0]
        if row_count == 0:
            continue

        t0 = time.monotonic()
        conn.execute(f"SELECT seaturtle_compress_partition('{part_name}')")
        conn.commit()
        elapsed = time.monotonic() - t0

        print(f"  Compressed {part_name}: {row_count:,} rows in {elapsed:.1f}s")
        results.append((part_name, elapsed))

    return results


# ---------------------------------------------------------------------------
# Query profiling (pg_seaturtle specific)
# ---------------------------------------------------------------------------

def run_explain_analyze(conn, queries):
    """Run EXPLAIN ANALYZE for each query and extract SeaTurtle timing/stats.

    Returns {qid: {"timing": str, "stats": str}} with the raw property values,
    or empty dict entries for queries that don't hit compressed partitions.
    """
    results = {}
    for qid, desc, sql in queries:
        try:
            rows = conn.execute(
                f"EXPLAIN (ANALYZE, COSTS OFF) {sql}"
            ).fetchall()
            explain_text = "\n".join(r[0] for r in rows)

            # Collect all SeaTurtle Timing/Stats lines (one per compressed partition)
            timings = []
            stats_lines = []
            for line in explain_text.split("\n"):
                line = line.strip()
                if line.startswith("SeaTurtle Timing:"):
                    timings.append(line.split(":", 1)[1].strip())
                elif line.startswith("SeaTurtle Stats:"):
                    stats_lines.append(line.split(":", 1)[1].strip())

            if not timings:
                results[qid] = {}
                continue

            # Sum timing values across partitions
            totals = {"metadata": 0.0, "heap_scan": 0.0, "decompress": 0.0, "batch_eval": 0.0, "emit": 0.0}
            for t in timings:
                for token in t.replace("(", "").replace(")", "").split():
                    if "=" in token:
                        k, v = token.split("=", 1)
                        if k in totals:
                            try:
                                totals[k] += float(v)
                            except ValueError:
                                pass

            total_ms = sum(totals.values())
            timing_str = (
                f"{total_ms:.3f} ms (metadata={totals['metadata']:.3f} "
                f"heap_scan={totals['heap_scan']:.3f} "
                f"decompress={totals['decompress']:.3f} "
                f"batch_eval={totals['batch_eval']:.3f} "
                f"emit={totals['emit']:.3f})"
            )

            # Sum stats across partitions
            stat_totals = {"segments": 0, "segments_skipped": 0, "rows_out": 0, "rows_filtered": 0, "rows_batch_filtered": 0, "compressed_bytes": 0}
            for s in stats_lines:
                for token in s.split():
                    if "=" in token:
                        k, v = token.split("=", 1)
                        if k in stat_totals:
                            try:
                                stat_totals[k] += int(v)
                            except ValueError:
                                pass
            stats_str = " ".join(f"{k}={v}" for k, v in stat_totals.items())

            results[qid] = {
                "timing": timing_str,
                "stats": stats_str,
                "partitions": len(timings),
            }
        except Exception as e:
            conn.rollback()
            results[qid] = {"error": str(e)}

    return results


# ---------------------------------------------------------------------------
# Results reporting
# ---------------------------------------------------------------------------

def print_query_results(uncompr_results, compr_results, profile_results=None):
    """Print markdown table of query performance.

    Accepts results in the format {qid: (median_ms, rows)}.
    If profile_results is provided, also prints SeaTurtle timing breakdown.
    """
    print("\n### Query Performance")
    print()
    print(f"| {'Query':<6} | {'Description':<25} | {'Uncompr (ms)':>13} | {'Compr (ms)':>11} | {'Ratio':>6} |")
    print(f"|{'-'*8}|{'-'*27}|{'-'*15}|{'-'*13}|{'-'*8}|")

    for qid, desc, _ in QUERIES:
        u = uncompr_results.get(qid, (float("inf"), None))[0]
        c = compr_results.get(qid, (float("inf"), None))[0]
        if u != float("inf") and c != float("inf") and c > 0:
            ratio = f"{u / c:.2f}x"
        else:
            ratio = "N/A"
        u_str = f"{u:.1f}" if u != float("inf") else "ERR"
        c_str = f"{c:.1f}" if c != float("inf") else "ERR"
        print(f"| {qid:<6} | {desc:<25} | {u_str:>13} | {c_str:>11} | {ratio:>6} |")

    if profile_results:
        print("\n### SeaTurtle Scan Timing Breakdown (EXPLAIN ANALYZE)")
        print()
        print(f"| {'Query':<6} | {'SeaTurtle Total':>13} | {'Metadata':>10} | {'Heap Scan':>10} | {'Decompress':>11} | {'Batch Eval':>10} | {'Emit':>10} | {'Stats':<65} |")
        print(f"|{'-'*8}|{'-'*15}|{'-'*12}|{'-'*12}|{'-'*13}|{'-'*12}|{'-'*12}|{'-'*67}|")

        for qid, desc, _ in QUERIES:
            info = profile_results.get(qid, {})
            if "error" in info:
                print(f"| {qid:<6} | {'ERR':>13} | {'':>10} | {'':>10} | {'':>11} | {'':>10} | {'':>10} | {info['error'][:65]:<65} |")
                continue
            timing = info.get("timing", "")
            stats = info.get("stats", "")
            if not timing:
                print(f"| {qid:<6} | {'n/a':>13} | {'':>10} | {'':>10} | {'':>11} | {'':>10} | {'':>10} | {'(no compressed scan)' :<65} |")
                continue

            # Parse timing: "X.XXX ms (metadata=X.XXX heap_scan=X.XXX decompress=X.XXX batch_eval=X.XXX emit=X.XXX)"
            parts = {}
            for token in timing.replace("(", "").replace(")", "").split():
                if "=" in token:
                    k, v = token.split("=", 1)
                    parts[k] = v

            total_str = timing.split(" ms")[0].strip() if " ms" in timing else timing
            print(f"| {qid:<6} | {total_str + ' ms':>13} | {parts.get('metadata', ''):>10} | {parts.get('heap_scan', ''):>10} | {parts.get('decompress', ''):>11} | {parts.get('batch_eval', ''):>10} | {parts.get('emit', ''):>10} | {stats[:65]:<65} |")


def print_compression_stats(conn):
    """Print markdown table of per-partition compression stats."""
    stats = conn.execute(
        "SELECT partition_name, raw_size, compressed_size, compression_ratio, row_count "
        "FROM seaturtle_compression_stats('hits') "
        "WHERE compressed_size IS NOT NULL "
        "ORDER BY partition_name"
    ).fetchall()

    if not stats:
        print("\n(No compression stats available)")
        return

    print("\n### Compression Stats")
    print()
    print(f"| {'Partition':<20} | {'Raw (MB)':>9} | {'Compr (MB)':>11} | {'Ratio':>6} | {'Rows':>10} |")
    print(f"|{'-'*22}|{'-'*11}|{'-'*13}|{'-'*8}|{'-'*12}|")

    total_raw = 0
    total_comp = 0
    total_rows = 0

    for part_name, raw, comp, ratio, rows in stats:
        raw_mb = (raw or 0) / 1e6
        comp_mb = (comp or 0) / 1e6
        ratio_str = f"{ratio:.1f}x" if ratio else "N/A"
        rows_val = rows or 0
        print(f"| {part_name:<20} | {raw_mb:>9.1f} | {comp_mb:>11.1f} | {ratio_str:>6} | {rows_val:>10,} |")
        total_raw += raw or 0
        total_comp += comp or 0
        total_rows += rows_val

    total_ratio = total_raw / total_comp if total_comp > 0 else 0
    print(f"| {'TOTAL':<20} | {total_raw / 1e6:>9.1f} | {total_comp / 1e6:>11.1f} | {total_ratio:.1f}x | {total_rows:>10,} |")


# ---------------------------------------------------------------------------
# Pytest fixtures & test class
# ---------------------------------------------------------------------------

@pytest.fixture(scope="class")
def clickbench_db(pg_container):
    """Create a database, load ClickBench data, enable compression.

    Scoped to class so data is loaded once for all benchmark tests.
    """
    import uuid

    from conftest import HOST_PORT, PG_PASSWORD, PG_USER, _admin_conn

    db_name = "bench_clickbench_" + uuid.uuid4().hex[:8]

    admin = _admin_conn()
    admin.execute(f'CREATE DATABASE "{db_name}"')
    admin.close()

    conn = psycopg.connect(
        host="localhost",
        port=HOST_PORT,
        user=PG_USER,
        password=PG_PASSWORD,
        dbname=db_name,
    )
    conn.execute("CREATE EXTENSION pg_seaturtle")
    conn.execute("SET jit = off")
    conn.commit()

    # Setup: create table, partition, load data
    row_count = setup_clickbench(conn, NUM_FILES)
    enable_compression(conn)

    yield conn

    conn.close()
    if not os.environ.get("KEEP_CONTAINER"):
        admin = _admin_conn()
        admin.execute(f'DROP DATABASE "{db_name}"')
        admin.close()
    else:
        print(f"\n  KEEP_CONTAINER set — keeping database {db_name}")


class TestClickBench:
    """ClickBench real-world benchmark for pg_seaturtle compression."""

    def test_benchmark(self, clickbench_db):
        """Run full benchmark: uncompressed queries, compress, compressed queries."""
        conn = clickbench_db

        # Phase 1: Query uncompressed data
        print("\n\n=== Phase 1: Uncompressed Queries ===")
        uncompr_results = run_queries(conn, QUERIES, label="uncompr")

        # Phase 2: Compress all partitions
        print("\n=== Phase 2: Compressing Partitions ===")
        compress_timings = compress_all_partitions(conn)
        total_compress_time = sum(t for _, t in compress_timings)
        print(f"\n  Total compression time: {total_compress_time:.1f}s "
              f"({len(compress_timings)} partitions)")

        # Diagnostic: verify basic query works after compression
        print("\n=== Diagnostic: Post-compression check ===")
        try:
            count = conn.execute("SELECT count(*) FROM hits").fetchone()[0]
            print(f"  count(*) = {count}")
        except Exception as e:
            print(f"  count(*) FAILED: {e}")
            conn.rollback()

        try:
            plan = conn.execute("EXPLAIN SELECT count(*) FROM hits").fetchall()
            for row in plan:
                print(f"  {row[0]}")
        except Exception as e:
            print(f"  EXPLAIN FAILED: {e}")
            conn.rollback()

        # Phase 3: Query compressed data
        print("\n=== Phase 3: Compressed Queries ===")
        compr_results = run_queries(conn, QUERIES, label="compr")

        # Phase 4: Profile compressed queries with EXPLAIN ANALYZE
        print("\n=== Phase 4: Profiling Compressed Queries (EXPLAIN ANALYZE) ===")
        profile_results = run_explain_analyze(conn, QUERIES)
        for qid, info in profile_results.items():
            if "error" in info:
                print(f"  {qid}: ERROR - {info['error']}")
            elif "timing" in info:
                print(f"  {qid}: {info['timing']}")
                if "stats" in info:
                    print(f"         {info['stats']}")
            else:
                print(f"  {qid}: (no compressed scan)")

        # Phase 5: Validate compressed results match uncompressed
        print("\n=== Phase 5: Validating Results ===")
        mismatches = []
        for qid, desc, _ in QUERIES:
            u_timing, u_rows = uncompr_results.get(qid, (float("inf"), None))
            c_timing, c_rows = compr_results.get(qid, (float("inf"), None))

            if u_rows is None or c_rows is None:
                print(f"  {qid}: SKIP (query errored)")
                continue

            if u_rows == c_rows:
                print(f"  {qid}: OK ({len(u_rows)} rows match)")
            else:
                mismatches.append(qid)
                print(f"  {qid}: MISMATCH!")
                print(f"    uncompressed: {len(u_rows)} rows, first={u_rows[:2]}")
                print(f"    compressed:   {len(c_rows)} rows, first={c_rows[:2]}")

        # Phase 6: Print results
        print("\n\n" + "=" * 72)
        print("  ClickBench Benchmark Results")
        print(f"  Files: {NUM_FILES}, Warmup: {WARMUP_RUNS}, Timed runs: {TIMED_RUNS}")
        print("=" * 72)

        print_query_results(uncompr_results, compr_results, profile_results)
        print_compression_stats(conn)

        # Save results for cross-system comparison
        totals = conn.execute(
            "SELECT sum(raw_size), sum(compressed_size) "
            "FROM seaturtle_compression_stats('hits') "
            "WHERE compressed_size IS NOT NULL"
        ).fetchone()
        raw_bytes = int(totals[0] or 0)
        compressed_bytes = int(totals[1] or 0)
        save_bench_results("pg_seaturtle", {
            "uncompressed_queries": query_results_to_dict(uncompr_results),
            "compressed_queries": query_results_to_dict(compr_results),
            "raw_bytes": raw_bytes,
            "compressed_bytes": compressed_bytes,
            "compression_ratio": raw_bytes / compressed_bytes if compressed_bytes > 0 else 0,
            "compression_time_s": total_compress_time,
        })

        assert not mismatches, (
            f"Result mismatch for queries: {mismatches}. "
            "Compressed query results differ from uncompressed."
        )
