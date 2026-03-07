#!/usr/bin/env python3
"""Cross-system benchmark comparison.

Reads JSON result files from tests/.bench_results/ (written by
bench_clickbench.py and bench_clickbench_timescaledb.py) and prints
unified markdown comparison tables.

Run standalone:
    python tests/bench_compare.py

Or via Makefile:
    make bench-compare
"""

import json
import math
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from clickbench_queries import QUERIES

RESULTS_DIR = Path(__file__).parent / ".bench_results"


def load_results():
    """Load all available JSON result files. Returns {system_name: data}."""
    results = {}
    if not RESULTS_DIR.exists():
        return results
    for f in sorted(RESULTS_DIR.glob("*.json")):
        with open(f) as fh:
            results[f.stem] = json.load(fh)
    return results


def geometric_mean(values):
    """Geometric mean of positive values, ignoring None."""
    vals = [v for v in values if v is not None and v > 0]
    if not vals:
        return None
    return math.exp(sum(math.log(v) for v in vals) / len(vals))


def fmt_ms(val):
    """Format a millisecond value, handling None."""
    if val is None:
        return "-"
    return f"{val:.1f}"


def fmt_mb(val):
    """Format bytes as MB."""
    if val is None or val == 0:
        return "-"
    return f"{val / 1e6:.1f}"


def fmt_ratio(val):
    """Format compression ratio."""
    if val is None or val == 0:
        return "-"
    return f"{val:.1f}x"


def fmt_time(val):
    """Format seconds."""
    if val is None:
        return "-"
    return f"{val:.1f}"


def fmt_speedup(a, b):
    """Format speedup of b over a (b/a). >1 means a is faster."""
    if a is None or b is None or a <= 0:
        return "-"
    return f"{b / a:.2f}x"


def print_compression_table(results):
    """Table 1: Compression comparison across systems."""
    rows = []

    seaturtle = results.get("pg_seaturtle")
    if seaturtle:
        rows.append((
            "pg_seaturtle",
            seaturtle.get("raw_bytes"),
            seaturtle.get("compressed_bytes"),
            seaturtle.get("compression_ratio"),
            seaturtle.get("compression_time_s"),
        ))

    tsdb = results.get("timescaledb_tsl")
    if tsdb:
        rows.append((
            "TimescaleDB (matching)",
            tsdb.get("raw_bytes"),
            tsdb.get("compressed_matching_bytes"),
            tsdb.get("compression_ratio_matching"),
            tsdb.get("compression_time_matching_s"),
        ))
        rows.append((
            "TimescaleDB (default)",
            tsdb.get("raw_bytes"),
            tsdb.get("compressed_default_bytes"),
            tsdb.get("compression_ratio_default"),
            tsdb.get("compression_time_default_s"),
        ))

    if not rows:
        return

    print("\n### Compression Comparison")
    print()
    print(f"| {'System':<24} | {'Raw (MB)':>9} | {'Compressed (MB)':>16} | {'Ratio':>6} | {'Compr Time (s)':>15} |")
    print(f"|{'-'*26}|{'-'*11}|{'-'*18}|{'-'*8}|{'-'*17}|")

    for name, raw, comp, ratio, ctime in rows:
        print(f"| {name:<24} | {fmt_mb(raw):>9} | {fmt_mb(comp):>16} | {fmt_ratio(ratio):>6} | {fmt_time(ctime):>15} |")


def print_query_table(results):
    """Table 2: Query performance comparison across systems."""
    seaturtle = results.get("pg_seaturtle")
    tsdb = results.get("timescaledb_tsl")

    # Build column list dynamically based on available data
    columns = []  # (header, getter)

    if seaturtle:
        columns.append(("SeaTurtle Uncompr", lambda qid: seaturtle.get("uncompressed_queries", {}).get(qid)))
        columns.append(("SeaTurtle Compr", lambda qid: seaturtle.get("compressed_queries", {}).get(qid)))
    if tsdb:
        columns.append(("TSDB Uncompr", lambda qid: tsdb.get("uncompressed_queries", {}).get(qid)))
        columns.append(("TSDB Match", lambda qid: tsdb.get("compressed_matching_queries", {}).get(qid)))
        columns.append(("TSDB Default", lambda qid: tsdb.get("compressed_default_queries", {}).get(qid)))

    if not columns:
        return

    print("\n### Query Performance Comparison (ms)")
    print()

    # Header
    header = f"| {'Query':<6} | {'Description':<25} |"
    sep = f"|{'-'*8}|{'-'*27}|"
    for col_name, _ in columns:
        header += f" {col_name:>14} |"
        sep += f"{'-'*16}|"
    print(header)
    print(sep)

    # Data rows
    col_values = {col_name: [] for col_name, _ in columns}
    for qid, desc, _ in QUERIES:
        row = f"| {qid:<6} | {desc:<25} |"
        for col_name, getter in columns:
            val = getter(qid)
            row += f" {fmt_ms(val):>14} |"
            col_values[col_name].append(val)
        print(row)

    # Geometric mean summary
    row = f"| {'':.<6} | {'Geometric Mean':<25} |"
    for col_name, _ in columns:
        gm = geometric_mean(col_values[col_name])
        row += f" {fmt_ms(gm):>14} |"
    print(row)


def print_speedup_table(results):
    """Table 3: Compressed query speedup (pg_seaturtle vs TimescaleDB matching)."""
    seaturtle = results.get("pg_seaturtle")
    tsdb = results.get("timescaledb_tsl")

    if not seaturtle or not tsdb:
        return

    seaturtle_compr = seaturtle.get("compressed_queries", {})
    tsdb_match = tsdb.get("compressed_matching_queries", {})

    if not seaturtle_compr or not tsdb_match:
        return

    print("\n### Compressed Query Speedup (pg_seaturtle vs TimescaleDB matching)")
    print()
    print(f"| {'Query':<6} | {'Description':<25} | {'SeaTurtle (ms)':>12} | {'TSDB (ms)':>10} | {'Speedup':>8} |")
    print(f"|{'-'*8}|{'-'*27}|{'-'*14}|{'-'*12}|{'-'*10}|")

    seaturtle_vals = []
    tsdb_vals = []
    for qid, desc, _ in QUERIES:
        c = seaturtle_compr.get(qid)
        t = tsdb_match.get(qid)
        speedup = fmt_speedup(c, t)
        print(f"| {qid:<6} | {desc:<25} | {fmt_ms(c):>12} | {fmt_ms(t):>10} | {speedup:>8} |")
        seaturtle_vals.append(c)
        tsdb_vals.append(t)

    # Summary
    gm_c = geometric_mean(seaturtle_vals)
    gm_t = geometric_mean(tsdb_vals)
    speedup = fmt_speedup(gm_c, gm_t)
    print(f"| {'':.<6} | {'Geometric Mean':<25} | {fmt_ms(gm_c):>12} | {fmt_ms(gm_t):>10} | {speedup:>8} |")


def main():
    results = load_results()
    if not results:
        print("No benchmark results found in tests/.bench_results/")
        print("Run 'make bench-clickbench' and/or 'make bench-timescaledb' first.")
        sys.exit(1)

    systems = ", ".join(results.keys())
    print(f"\n{'=' * 72}")
    print(f"  Cross-System Benchmark Comparison")
    print(f"  Systems: {systems}")
    print(f"{'=' * 72}")

    print_compression_table(results)
    print_query_table(results)
    print_speedup_table(results)

    print()


if __name__ == "__main__":
    main()
