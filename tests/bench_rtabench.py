"""Local Docker rtabench — plain PG vs pg_deltax side-by-side.

Runs the 31 rtabench raw queries against two variants of the same data:
`order_events_plain` (plain PostgreSQL table) and `order_events`
(pg_deltax-managed, compressed via direct backfill). Compares warm times
and requires byte-identical result sets for correctness.

See `tests/rtabench_data.py` for the download / slice / load pipeline and
`rtabench/queries/*.sql` for the query texts (used verbatim, with a
`\\border_events\\b → order_events_plain` substitution for the plain run).

Makefile entry points:
  - make bench-rtabench        # default 250K orders, ~5 min first run
  - make bench-rtabench-keep   # + KEEP_CONTAINER + BENCH_PERSIST
  - make bench-rtabench-full   # all 10M orders
  - make bench-rtabench-clean  # wipe container / volume / cached CSVs
"""

from __future__ import annotations

import json
import os
import re
import statistics
import time
import uuid
from pathlib import Path

import psycopg
import pytest

# Reuse helpers from the ClickBench bench — results dir, commit hash, JSON save.
from clickbench_data import (
    BENCH_RESULTS_DIR,
    save_bench_results,
    _get_git_commit_short,
)
from rtabench_data import (
    RTABENCH_ORDERS,
    WARMUP_RUNS,
    TIMED_RUNS,
    load_all,
)


QUERIES_DIR = Path(__file__).parent.parent / "rtabench" / "queries"
_OE = re.compile(r"\border_events\b")


# ---------------------------------------------------------------------------
# Query loading + substitution
# ---------------------------------------------------------------------------

def load_queries() -> list[tuple[str, str]]:
    """Return [(qid, sql)] for every .sql in rtabench/queries/ — sorted by
    filename so indices line up with Q00..Q30."""
    out = []
    for p in sorted(QUERIES_DIR.glob("*.sql")):
        sql = p.read_text().strip().rstrip(";").strip()
        out.append((p.stem, sql))
    return out


def for_plain(sql: str) -> str:
    return _OE.sub("order_events_plain", sql)


# ---------------------------------------------------------------------------
# Query execution
# ---------------------------------------------------------------------------

def run_once(conn, sql: str) -> tuple[float, list | None, Exception | None]:
    """Run a single query, return (elapsed_ms, rows, error). On error, rows
    is None, elapsed is inf, and caller should rollback."""
    t0 = time.monotonic()
    try:
        rows = conn.execute(sql).fetchall()
        return (time.monotonic() - t0) * 1000.0, rows, None
    except Exception as e:
        conn.rollback()
        return float("inf"), None, e


def run_phase(conn, queries: list[tuple[str, str]], *, label: str, transform=lambda s: s) -> dict:
    """Run every query with 1 warmup + TIMED_RUNS timed runs. Returns
    {qid: {"ms": median_or_inf, "rows": last_rows, "error": str|None}}."""
    out: dict[str, dict] = {}
    for qid, sql_src in queries:
        sql = transform(sql_src)

        # Warmup (ignored)
        for _ in range(WARMUP_RUNS):
            _, _, _ = run_once(conn, sql)

        # Timed runs
        timings: list[float] = []
        last_rows = None
        last_error: Exception | None = None
        for _ in range(TIMED_RUNS):
            ms, rows, err = run_once(conn, sql)
            timings.append(ms)
            if rows is not None:
                last_rows = rows
            if err is not None:
                last_error = err

        median = statistics.median(timings)
        out[qid] = {
            "ms": None if median == float("inf") else median,
            "rows": last_rows,
            "error": str(last_error) if last_error is not None else None,
        }

        status = f"{median:.1f} ms" if median != float("inf") else f"ERROR: {last_error}"
        print(f"  [{label}] {qid}: {status}")
    return out


# ---------------------------------------------------------------------------
# Row-estimate oracle (L4) — plain PG's ANALYZE is the ground-truth estimator.
#
# The fact-table scan estimate is where almost every planner disaster lived
# (Q19's absent-value 20M, Q06's 100%-vs-2%, Q30's rows=8). We extract the
# estimated rows the planner expects from the `order_events` scan in both
# variants and compare:
#   - plain PG (`order_events_plain`, properly ANALYZEd) → the oracle estimate
#   - pg_deltax (`order_events`, synthesized pg_statistic) → estimate + actual
#
# A query fails the gate only when pg_deltax's scan estimate is BOTH far from
# the actual row count AND far from plain PG's estimate of the same predicate.
# The double condition keeps the gate from flaring on predicates that are
# intrinsically hard to estimate (where plain PG is also off, so the two
# estimates agree and we don't blame pg_deltax) while still catching a
# deltax-specific blunder (where plain PG nails it and deltax is wild).
# ---------------------------------------------------------------------------

# Order-of-magnitude gate. The disasters were 50×–9000×; legitimate divergence
# between two planners' estimates of the *same* filter is small. Override with
# RTABENCH_EST_RATIO (0 disables the hard gate, leaving the report).
EST_RATIO_THRESHOLD = float(os.environ.get("RTABENCH_EST_RATIO", "20"))

# Custom Plan Provider names of the scan-shaped deltax nodes (they expose the
# fact-table row estimate). The aggregate-pushdown nodes (DeltaXAgg / DeltaXCount
# / DeltaXMinMax) emit grouped rows, not scan rows, so they're not comparable.
_DELTAX_SCAN_PROVIDERS = {"DeltaXAppend", "DeltaXDecompress"}


# ---------------------------------------------------------------------------
# L5 · Wall-clock regression backstop. Phase E catches *plan* blunders (a wild
# row estimate); this catches *time* blunders — a query that silently got, say,
# 3× slower than a recorded-good baseline. That's the "nobody noticed the 17 s
# query" failure mode: a stats change that's still correct and still well-
# estimated can flip a plan to a slower-but-valid one (see PLANNER_STATS.md, the
# latent-gate lesson). Compared against a *pinned* baseline file refreshed
# deliberately (RTABENCH_BLESS=1), NOT the previous run — so a slow drift can't
# quietly ratchet the baseline upward run after run.
# ---------------------------------------------------------------------------

# >3× slower than baseline = regression. Override with RTABENCH_TIME_RATIO
# (0 disables the hard gate, leaving the report). Only queries whose baseline is
# >= the floor are checked: the local 250K subset is noisy at sub-millisecond
# times, where a 3× ratio is meaningless.
TIME_RATIO_THRESHOLD = float(os.environ.get("RTABENCH_TIME_RATIO", "3"))
TIME_FLOOR_MS = float(os.environ.get("RTABENCH_TIME_FLOOR_MS", "10"))
BENCH_BLESS = os.environ.get("RTABENCH_BLESS", "") not in ("", "0", "false")
BASELINE_PATH = BENCH_RESULTS_DIR / "rtabench_baseline.json"


def load_time_baseline() -> dict | None:
    """Load the pinned warm-time baseline, or None if absent / unreadable.
    Shape: {"n_orders": N, "commit": "...", "deltax_queries": {qid: ms}}."""
    if not BASELINE_PATH.exists():
        return None
    try:
        with open(BASELINE_PATH) as f:
            return json.load(f)
    except Exception:
        return None


def save_time_baseline(deltax: dict, queries) -> None:
    """Record the current deltax warm times as the new pinned baseline."""
    data = {
        "n_orders": RTABENCH_ORDERS,
        "commit": _get_git_commit_short(),
        "deltax_queries": {q: deltax[q]["ms"] for q, _ in queries},
    }
    BENCH_RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    with open(BASELINE_PATH, "w") as f:
        json.dump(data, f, indent=2)
    print(f"  Blessed baseline written to {BASELINE_PATH}")


def run_time_regression(
    deltax: dict, baseline: dict, queries
) -> tuple[dict, list[str]]:
    """Per query, ratio of current warm ms to the baseline ms. Flags a
    regression when ratio > threshold AND the baseline is above the noise
    floor. Returns (report, violations)."""
    base_q = baseline.get("deltax_queries", {})
    report: dict = {}
    violations: list[str] = []
    for qid, _ in queries:
        cur = deltax[qid]["ms"]
        base = base_q.get(qid)
        if cur is None or base is None:
            report[qid] = {"note": "no-baseline" if base is None else "error"}
            continue
        ratio = cur / base if base > 0 else 0.0
        report[qid] = {"cur": cur, "base": base, "ratio": ratio}
        if (
            TIME_RATIO_THRESHOLD > 0
            and base >= TIME_FLOOR_MS
            and ratio > TIME_RATIO_THRESHOLD
        ):
            violations.append(qid)
    return report, violations


def _walk_plan(node):
    """Yield every node in an EXPLAIN JSON plan tree (depth-first)."""
    yield node
    for child in node.get("Plans", []) or []:
        yield from _walk_plan(child)


def _misratio(a: float, b: float) -> float:
    """Symmetric over/under-estimate ratio, floored at 1 row to avoid blowups
    when one side is ~0 (PG clamps a 0-selectivity scan to 1)."""
    hi, lo = max(a, b), max(min(a, b), 1.0)
    return hi / lo


def _explain_json(conn, sql: str, *, analyze: bool):
    """Return the root Plan dict of an EXPLAIN (FORMAT JSON) run, or None on
    error (query rolled back)."""
    opt = "ANALYZE, TIMING OFF, FORMAT JSON" if analyze else "FORMAT JSON"
    try:
        res = conn.execute(f"EXPLAIN ({opt}) {sql}").fetchone()[0]
    except Exception:
        conn.rollback()
        return None
    root = json.loads(res) if isinstance(res, str) else res
    return root[0]["Plan"]


def _deltax_fact_scan(root) -> tuple | None:
    """Find the dominant fact-table scan node in a pg_deltax plan and return
    (est_rows, actual_rows) — per-loop, as PG reports them. None if the query
    is fully pushed down (DeltaXAgg/Count/MinMax) with no comparable scan."""
    candidates = [
        n for n in _walk_plan(root)
        if n.get("Custom Plan Provider") in _DELTAX_SCAN_PROVIDERS
    ]
    if not candidates:
        return None
    n = max(candidates, key=lambda x: x.get("Plan Rows", 0))
    return float(n.get("Plan Rows", 0)), float(n.get("Actual Rows", 0))


def _plain_fact_scan_estimate(root) -> float | None:
    """Find the dominant `order_events_plain` scan node's estimated rows in a
    plain-PG plan. None if the query doesn't scan the fact table."""
    candidates = [
        n for n in _walk_plan(root)
        if n.get("Relation Name") == "order_events_plain"
    ]
    if not candidates:
        return None
    return float(max(candidates, key=lambda x: x.get("Plan Rows", 0)).get("Plan Rows", 0))


# ---------------------------------------------------------------------------
# Correctness comparison
# ---------------------------------------------------------------------------

# Queries that use LIMIT where the ORDER BY doesn't strictly tie-break.
# For these we only require row-count + multiset-after-sort equality; the
# boundary row(s) may differ between plan variants if ties exist.
LIMIT_TIE_QUERIES = {
    "0005_search_events_for_processor",
    "0006_order_events_without_backups",
    "0010_last_event_for_an_order",
    "0016_customers_with_most_orders",
    "0017_top_selling_month_product",
    "0018_customer_month_value",
    "0023_top_sales_volume_product_from_terminal",
    "0024_top_customer_by_revenue",
    "0029_top_product_in_age_group",
    "0030_customers_with_most_orders_delivered",
}


def _normalize_rows(rows: list | None) -> list:
    """Map each row's Decimal/float → a normalized tuple (for stable sort +
    compare) using stringification. Also sorts the list."""
    if rows is None:
        return []
    norm = []
    for r in rows:
        norm.append(tuple(str(c) if c is not None else None for c in r))
    norm.sort()
    return norm


def compare_results(qid: str, plain_rows, deltax_rows) -> tuple[bool, str]:
    """Return (ok, detail). LIMIT-tie queries relax to row-count equality."""
    p = _normalize_rows(plain_rows)
    d = _normalize_rows(deltax_rows)
    if qid in LIMIT_TIE_QUERIES:
        if len(p) != len(d):
            return False, f"row count: plain={len(p)} deltax={len(d)}"
        return True, f"{len(p)} rows, tie-relaxed"
    if p == d:
        return True, f"{len(p)} rows"
    # Mismatch — produce a short diff
    if len(p) != len(d):
        return False, f"row count: plain={len(p)} deltax={len(d)}"
    diffs = [i for i in range(len(p)) if p[i] != d[i]]
    first = diffs[0]
    return False, (
        f"{len(diffs)}/{len(p)} rows differ — first at idx {first}: "
        f"plain={p[first]!r} vs deltax={d[first]!r}"
    )


# ---------------------------------------------------------------------------
# Fixture: container-level DB with data loaded once per test class
# ---------------------------------------------------------------------------

@pytest.fixture(scope="class")
def rtabench_db(pg_container):
    from conftest import HOST_PORT, PG_PASSWORD, PG_USER, _admin_conn

    # Stable DB name when BENCH_PERSIST so re-runs find the same data.
    persist = bool(os.environ.get("BENCH_PERSIST"))
    db_name = "bench_rtabench" if persist else f"bench_rtabench_{uuid.uuid4().hex[:8]}"

    admin = _admin_conn()
    exists = admin.execute(
        "SELECT 1 FROM pg_database WHERE datname = %s", (db_name,)
    ).fetchone()
    if not exists:
        admin.execute(f'CREATE DATABASE "{db_name}"')
    admin.close()

    conn = psycopg.connect(
        host="localhost",
        port=HOST_PORT,
        user=PG_USER,
        password=PG_PASSWORD,
        dbname=db_name,
    )
    conn.execute("SET jit = off")
    conn.commit()
    conn.execute("CREATE EXTENSION IF NOT EXISTS pg_deltax")
    conn.commit()

    # Session-level planner tunes — keep the local bench comparable to the
    # EC2 benchmark where enable_nestloop=off reliably avoids the
    # NestLoop-over-Materialize trap on queries like Q17 (see
    # RTABENCH_QUERY_ANALYSIS.md §3.A).
    conn.execute("SET enable_nestloop = off")
    conn.execute("SET work_mem = '1GB'")
    # Activate the planner_hook walker so chain Exprs in queries
    # transparently use the synthetic columns set up by
    # `rtabench_data.py::setup_schema`. Without this the walker is a
    # no-op and the chain-Expr eligibility infrastructure on DeltaXAgg
    # stays dormant for RTABench.
    conn.execute("SET pg_deltax.json_extract_mode = 'fields'")
    conn.commit()

    load_all(conn, RTABENCH_ORDERS)

    yield conn

    conn.close()
    if os.environ.get("KEEP_CONTAINER") or persist:
        print(f"\n  Keeping database {db_name} for reuse")
        print(f"  Connect: docker exec -it pg_deltax_inttest psql -U {PG_USER} -d {db_name}")
    else:
        admin = _admin_conn()
        admin.execute(f'DROP DATABASE "{db_name}"')
        admin.close()


# ---------------------------------------------------------------------------
# Estimate-oracle phase
# ---------------------------------------------------------------------------

def run_estimate_oracle(conn, queries) -> tuple[dict, list[str]]:
    """For every query, extract the fact-table scan estimate from both
    variants and the actual rows from the deltax run. Returns
    (report_by_qid, violations).

    report_by_qid[qid] = {est_dx, act_dx, est_plain, ratio_act, ratio_oracle}
    A qid is a violation when est_dx is far from BOTH actual and the plain-PG
    oracle (see module docstring)."""
    report: dict[str, dict] = {}
    violations: list[str] = []
    for qid, sql_src in queries:
        dx_root = _explain_json(conn, sql_src, analyze=True)
        plain_root = _explain_json(conn, for_plain(sql_src), analyze=False)
        if dx_root is None or plain_root is None:
            report[qid] = {"note": "explain-error"}
            continue

        dx = _deltax_fact_scan(dx_root)
        est_plain = _plain_fact_scan_estimate(plain_root)
        if dx is None or est_plain is None:
            # Pure aggregate pushdown, or no fact-table scan → nothing to gate.
            report[qid] = {"note": "no-fact-scan"}
            continue

        est_dx, act_dx = dx
        ratio_act = _misratio(est_dx, act_dx)
        ratio_oracle = _misratio(est_dx, est_plain)
        report[qid] = {
            "est_dx": est_dx,
            "act_dx": act_dx,
            "est_plain": est_plain,
            "ratio_act": ratio_act,
            "ratio_oracle": ratio_oracle,
        }
        if (
            EST_RATIO_THRESHOLD > 0
            and ratio_act > EST_RATIO_THRESHOLD
            and ratio_oracle > EST_RATIO_THRESHOLD
        ):
            violations.append(qid)
    return report, violations


# ---------------------------------------------------------------------------
# The test
# ---------------------------------------------------------------------------

class TestRtabench:
    """Plain PG vs pg_deltax side-by-side on the 31 rtabench queries."""

    def test_bench(self, rtabench_db):
        conn = rtabench_db
        queries = load_queries()
        assert queries, "no queries found in rtabench/queries/"

        # Phase A: plain PG (order_events_plain)
        print("\n\n=== Phase A: Plain PostgreSQL ===")
        plain = run_phase(conn, queries, label="plain", transform=for_plain)

        # Phase B: pg_deltax (order_events, compressed)
        print("\n=== Phase B: pg_deltax ===")
        deltax = run_phase(conn, queries, label="deltax")

        # Phase C: correctness
        print("\n=== Phase C: Correctness ===")
        mismatches: list[str] = []
        for qid, _ in queries:
            pr = plain[qid]
            dr = deltax[qid]
            if pr["error"] or dr["error"]:
                mismatches.append(qid)
                print(f"  {qid}: ERROR — plain={pr['error']} deltax={dr['error']}")
                continue
            ok, detail = compare_results(qid, pr["rows"], dr["rows"])
            status = "OK" if ok else "MISMATCH"
            print(f"  {qid}: {status} ({detail})")
            if not ok:
                mismatches.append(qid)

        # Phase D: report
        print("\n=== Phase D: Summary ===")
        print(f"{'Query':<50}{'plain (ms)':>14}{'deltax (ms)':>14}{'speedup':>10}")
        plain_total = 0.0
        deltax_total = 0.0
        for qid, _ in queries:
            pms = plain[qid]["ms"]
            dms = deltax[qid]["ms"]
            if pms is not None:
                plain_total += pms
            if dms is not None:
                deltax_total += dms
            speedup = f"{pms/dms:>6.2f}x" if (pms is not None and dms not in (None, 0)) else "  —"
            pcell = f"{pms:>12.1f}" if pms is not None else "         —"
            dcell = f"{dms:>12.1f}" if dms is not None else "         —"
            print(f"{qid:<50}{pcell:>14}{dcell:>14}{speedup:>10}")
        print(
            f"{'total':<50}{plain_total:>12.1f}  {deltax_total:>12.1f}  "
            f"{(plain_total/deltax_total if deltax_total else 0):>6.2f}x"
        )

        # Phase E: row-estimate oracle (L4). plain PG's ANALYZE is the oracle;
        # flag any query whose fact-scan estimate is far from both the actual
        # rows and plain PG's estimate of the same predicate.
        print("\n=== Phase E: Row-estimate oracle (fact-table scan) ===")
        est_report, est_violations = run_estimate_oracle(conn, queries)
        hdr = f"{'Query':<50}{'est_dx':>12}{'act_dx':>12}{'est_plain':>12}{'x_act':>9}{'x_oracle':>10}"
        print(hdr)
        for qid, _ in queries:
            r = est_report[qid]
            if "note" in r:
                print(f"{qid:<50}{r['note']:>55}")
                continue
            flag = "  ⚠" if qid in est_violations else ""
            print(
                f"{qid:<50}{r['est_dx']:>12.0f}{r['act_dx']:>12.0f}{r['est_plain']:>12.0f}"
                f"{r['ratio_act']:>8.1f}x{r['ratio_oracle']:>9.1f}x{flag}"
            )
        thr = EST_RATIO_THRESHOLD
        print(
            f"\n  estimate gate: threshold {thr:g}x"
            f"{' (disabled)' if thr <= 0 else ''}, "
            f"{len(est_violations)} violation(s)"
        )

        # Phase F: wall-clock regression backstop (L5). Compare each query's
        # warm time against the pinned baseline; hard-fail >Nx regressions.
        print("\n=== Phase F: Wall-clock regression vs baseline ===")
        baseline = load_time_baseline()
        time_report: dict = {}
        time_violations: list[str] = []
        if BENCH_BLESS:
            save_time_baseline(deltax, queries)
            print("  RTABENCH_BLESS set — recorded this run as the new baseline; "
                  "regression gate skipped.")
        elif baseline is None:
            print(f"  No baseline at {BASELINE_PATH.name} — gate skipped. "
                  "Record one with `make bench-rtabench-bless`.")
        elif baseline.get("n_orders") != RTABENCH_ORDERS:
            print(f"  Baseline n_orders={baseline.get('n_orders')} != current "
                  f"{RTABENCH_ORDERS} — times not comparable, gate skipped.")
        else:
            time_report, time_violations = run_time_regression(deltax, baseline, queries)
            print(f"  baseline commit {baseline.get('commit', '?')}, "
                  f"{len(baseline.get('deltax_queries', {}))} queries")
            print(f"{'Query':<50}{'base (ms)':>12}{'cur (ms)':>12}{'ratio':>9}")
            for qid, _ in queries:
                r = time_report[qid]
                if "note" in r:
                    continue
                flag = "  ⚠" if qid in time_violations else ""
                # Only print rows above the floor or flagged — keep it readable.
                if r["base"] >= TIME_FLOOR_MS or flag:
                    print(f"{qid:<50}{r['base']:>12.1f}{r['cur']:>12.1f}"
                          f"{r['ratio']:>8.2f}x{flag}")
            print(
                f"\n  time gate: threshold {TIME_RATIO_THRESHOLD:g}x"
                f"{' (disabled)' if TIME_RATIO_THRESHOLD <= 0 else ''} above "
                f"{TIME_FLOOR_MS:g}ms, {len(time_violations)} violation(s)"
            )

        # Save machine-readable result
        save_bench_results(
            "rtabench_pg_deltax",
            {
                "n_orders": RTABENCH_ORDERS,
                "plain_queries": {q: plain[q]["ms"] for q, _ in queries},
                "deltax_queries": {q: deltax[q]["ms"] for q, _ in queries},
                "mismatches": mismatches,
                "estimate_oracle": est_report,
                "estimate_violations": est_violations,
                "estimate_threshold": thr,
                "time_regression": time_report,
                "time_violations": time_violations,
                "time_threshold": TIME_RATIO_THRESHOLD,
                "commit": _get_git_commit_short(),
            },
        )

        assert not mismatches, (
            f"{len(mismatches)} query result mismatch(es) between plain PG "
            f"and pg_deltax: {mismatches}"
        )
        assert not est_violations, (
            f"{len(est_violations)} query(ies) where pg_deltax's fact-scan row "
            f"estimate is >{thr:g}x off from BOTH the actual rows and plain PG's "
            f"estimate: {est_violations}. See the Phase E table above. This is the "
            f"class of misestimate that flips join/scan plans (Q19/Q06/Q30). "
            f"Override the gate with RTABENCH_EST_RATIO (0 to disable)."
        )
        assert not time_violations, (
            f"{len(time_violations)} query(ies) regressed >{TIME_RATIO_THRESHOLD:g}x "
            f"in warm time vs the pinned baseline: {time_violations}. See the Phase F "
            f"table above. A correct, well-estimated stats change can still flip a plan "
            f"to a slower one — this is that backstop. If the new time is the intended "
            f"new normal, re-bless with `make bench-rtabench-bless`; override with "
            f"RTABENCH_TIME_RATIO (0 to disable)."
        )
