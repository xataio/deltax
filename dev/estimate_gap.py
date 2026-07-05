#!/usr/bin/env python3
"""Empirical estimate-gap hunt.

For every query in a directory, run `EXPLAIN (ANALYZE, FORMAT JSON)` through a
running container's psql, walk the plan tree, and compute the per-node estimate
error (symmetric est/actual ratio, loop-aware). Reports the worst node per query
and, when a `--plain-sub` rewrite is given (RTABench's order_events ->
order_events_plain), runs the same query against the properly-ANALYZEd plain
table so deltax-specific errors (plain nails it, deltax is wild) stand out from
intrinsically-hard predicates (both off).

Usage:
  estimate_gap.py --container pg_deltax_inttest --db bench_rtabench \
      --queries rtabench/queries --plain-sub 'order_events=>order_events_plain'
"""
import argparse
import json
import re
import subprocess
import sys
from pathlib import Path


def psql_json(container, db, sql):
    """Run EXPLAIN (ANALYZE, FORMAT JSON) and return the parsed plan, or
    (None, error_string)."""
    explain = (
        "EXPLAIN (ANALYZE, FORMAT JSON, TIMING OFF, SUMMARY OFF, BUFFERS OFF) "
        + sql
    )
    proc = subprocess.run(
        ["docker", "exec", "-i", container, "psql", "-U", "postgres", "-d", db,
         "-tA", "-X", "-v", "ON_ERROR_STOP=1", "-c", explain],
        capture_output=True, text=True,
    )
    if proc.returncode != 0:
        return None, proc.stderr.strip().splitlines()[-1] if proc.stderr else "error"
    try:
        return json.loads(proc.stdout)[0]["Plan"], None
    except Exception as e:
        return None, f"parse: {e}"


def walk(node):
    yield node
    for child in node.get("Plans", []) or []:
        yield from walk(child)


def ratio(est, act):
    """Symmetric over/under ratio, floored at 1 row (PG clamps 0-sel to 1)."""
    hi, lo = max(est, act), max(min(est, act), 1.0)
    return hi / lo


def worst_node(plan, min_rows):
    """Return (max_ratio, node_dict) over nodes where the larger of est/act is
    >= min_rows — so we rank genuine large-cardinality divergences, not the
    est-vs-0 flooring noise from highly selective filters that match nothing on
    a small subset. Plan Rows and Actual Rows are both per-loop figures."""
    best = (1.0, None)
    for n in walk(plan):
        est = float(n.get("Plan Rows", 0))
        act = float(n.get("Actual Rows", 0))
        if max(est, act) < min_rows:
            continue
        r = ratio(est, act)
        if r > best[0]:
            best = (r, n)
    return best


def node_label(n):
    if n is None:
        return "(no mis-estimate)"
    parts = [n.get("Node Type", "?")]
    for k in ("Relation Name", "Custom Plan Provider", "Index Name"):
        if n.get(k):
            parts.append(n[k])
    return " ".join(parts)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--container", required=True)
    ap.add_argument("--db", required=True)
    ap.add_argument("--queries", required=True)
    ap.add_argument("--plain-sub", default=None,
                    help="'A=>B': also run with A replaced by B (the oracle)")
    ap.add_argument("--min-rows", type=float, default=100,
                    help="ignore nodes whose larger of est/act is below this")
    args = ap.parse_args()

    sub = None
    if args.plain_sub:
        a, b = args.plain_sub.split("=>")
        sub = (re.compile(r"\b" + re.escape(a) + r"\b"), b)

    rows = []
    for p in sorted(Path(args.queries).glob("*.sql")):
        sql = p.read_text().strip().rstrip(";").strip()
        plan, err = psql_json(args.container, args.db, sql)
        if err:
            rows.append((p.stem, None, None, None, None, err))
            continue
        dr, dn = worst_node(plan, args.min_rows)
        pr = None
        if sub:
            psql = sub[0].sub(sub[1], sql)
            pplan, perr = psql_json(args.container, args.db, psql)
            if not perr:
                pr, _ = worst_node(pplan, args.min_rows)
        rows.append((p.stem, dr, dn, pr, plan, None))

    # Rank by deltax worst-node ratio.
    rows.sort(key=lambda r: (r[1] or 0), reverse=True)
    print(f"{'query':<44}{'dx_err':>8}{'plain':>8}{'spec':>6}  {'est':>9} {'act':>9}  worst node")
    for qid, dr, dn, pr, _plan, err in rows:
        if err:
            print(f"{qid:<44}{'ERR':>8}  {err[:60]}")
            continue
        spec = ""
        if pr is not None and dr is not None:
            # deltax-specific if deltax error is big AND much worse than plain.
            spec = "yes" if (dr >= 5 and dr >= 3 * max(pr, 1.0)) else ""
        pr_s = f"{pr:>7.1f}x" if pr is not None else "       —"
        est = int(dn.get("Plan Rows", 0)) if dn else 0
        act = int(dn.get("Actual Rows", 0)) if dn else 0
        print(f"{qid:<44}{dr:>7.1f}x{pr_s:>8}{spec:>6}  {est:>9} {act:>9}  {node_label(dn)}")


if __name__ == "__main__":
    main()
