#!/usr/bin/env python3
"""ClickBench correctness verify / reference-build CLI.

Two modes:

  --build-reference <results_dir>  Build clickbench/reference_results.json
                                   from the captured CSVs (typically produced
                                   by capture_results.sh on a vanilla-PG EC2).

  --check <results_dir>            Diff captured CSVs against the committed
                                   reference. Exits non-zero on any mismatch.

The CSVs follow the format emitted by `COPY (...) TO STDOUT WITH (FORMAT csv,
DELIMITER E'\\t', NULL '\\N')`: tab-separated, CSV-quoted, with the literal
string '\\N' representing NULL.

Comparison semantics — per-query (deterministic / non-deterministic /
limit-tie) — come from tests/clickbench_queries.py so the verify path uses
exactly the same rules as the local pgrx bench's Phase 5 check.
"""

import argparse
import csv
import json
import re
import sys
from datetime import date
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "tests"))

from clickbench_queries import QUERIES, compare_results  # noqa: E402

NULL_TOKEN = "\\N"


def _qid_for_index(idx: int) -> str:
    return f"Q{idx}"


def _csv_path_for_index(results_dir: Path, idx: int) -> Path:
    return results_dir / f"q{idx:02d}.csv"


def _err_path_for_index(results_dir: Path, idx: int) -> Path:
    return results_dir / f"q{idx:02d}.err"


def _load_rows(csv_path: Path) -> list[list[str]]:
    """Parse a captured CSV. NULL tokens stay as the literal '\\N'."""
    if csv_path.stat().st_size == 0:
        return []
    with csv_path.open(newline="") as f:
        reader = csv.reader(f, delimiter="\t", quotechar='"', doublequote=True)
        return [list(row) for row in reader]


def _stable_rows(rows: list[list[str]]) -> list[tuple[str, ...]]:
    """Convert to hashable tuples for comparison."""
    return [tuple(r) for r in rows]


def build_reference(results_dir: Path, output: Path, stats_env: Path | None) -> int:
    metadata: dict[str, object] = {
        "dataset": "hits_compatible/hits.tsv (vanilla PostgreSQL)",
        "generated": date.today().isoformat(),
    }
    if stats_env is not None and stats_env.exists():
        for line in stats_env.read_text().splitlines():
            m = re.match(r"^([A-Z_]+)=(.*)$", line.strip())
            if m:
                metadata[m.group(1).lower()] = m.group(2)

    results: dict[str, list[list[str]]] = {}
    errors: list[str] = []
    for idx, (qid, _desc, _sql) in enumerate(QUERIES):
        err = _err_path_for_index(results_dir, idx)
        if err.exists():
            errors.append(f"{qid}: {err.read_text().strip()[:200]}")
            continue
        csv_path = _csv_path_for_index(results_dir, idx)
        if not csv_path.exists():
            errors.append(f"{qid}: missing {csv_path.name}")
            continue
        results[qid] = _load_rows(csv_path)

    if errors:
        print("Cannot build reference — the following queries errored:", file=sys.stderr)
        for e in errors:
            print(f"  {e}", file=sys.stderr)
        return 1

    payload = {"metadata": metadata, "results": results}
    output.write_text(json.dumps(payload, indent=2, ensure_ascii=False))
    total_rows = sum(len(rs) for rs in results.values())
    print(f"Wrote {output} ({len(results)} queries, {total_rows} total rows, "
          f"{output.stat().st_size / 1024:.1f} KiB)")
    return 0


def check(results_dir: Path, reference: Path) -> int:
    payload = json.loads(reference.read_text())
    expected_results = payload["results"]
    metadata = payload.get("metadata", {})
    print(f"Reference: {reference}")
    for k, v in metadata.items():
        print(f"  {k}: {v}")
    print()

    mismatches: list[str] = []
    missing: list[str] = []
    for idx, (qid, _desc, _sql) in enumerate(QUERIES):
        err = _err_path_for_index(results_dir, idx)
        if err.exists():
            print(f"  {qid}: SKIP (query errored: {err.read_text().strip()[:80]})")
            missing.append(qid)
            continue
        csv_path = _csv_path_for_index(results_dir, idx)
        if not csv_path.exists():
            print(f"  {qid}: SKIP (missing {csv_path.name})")
            missing.append(qid)
            continue
        if qid not in expected_results:
            print(f"  {qid}: SKIP (no reference entry)")
            missing.append(qid)
            continue

        actual = _stable_rows(_load_rows(csv_path))
        expected = _stable_rows(expected_results[qid])
        outcome = compare_results(qid, expected, actual)
        if outcome.ok:
            print(f"  {qid}: OK ({outcome.detail})")
        else:
            print(f"  {qid}: MISMATCH ({outcome.detail})")
            for line in outcome.extra_lines:
                print(f"    {line}")
            mismatches.append(qid)

    print()
    print(f"Summary: {len(QUERIES) - len(mismatches) - len(missing)} OK, "
          f"{len(mismatches)} mismatch, {len(missing)} skipped")
    if mismatches:
        print(f"  Mismatched queries: {', '.join(mismatches)}")
        return 1
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="mode", required=True)

    p_build = sub.add_parser("build-reference", help="Build reference JSON from captured CSVs")
    p_build.add_argument("results_dir", type=Path, help="Directory containing qNN.csv files")
    p_build.add_argument("-o", "--output", type=Path,
                         default=REPO_ROOT / "clickbench" / "reference_results.json",
                         help="Output JSON path")
    p_build.add_argument("--stats-env", type=Path, default=None,
                         help="Optional reference_stats.env file (ROW_COUNT, PG_VERSION)")

    p_check = sub.add_parser("check", help="Diff captured CSVs against reference JSON")
    p_check.add_argument("results_dir", type=Path, help="Directory containing qNN.csv files")
    p_check.add_argument("-r", "--reference", type=Path,
                         default=REPO_ROOT / "clickbench" / "reference_results.json",
                         help="Reference JSON path")

    args = parser.parse_args()

    if args.mode == "build-reference":
        return build_reference(args.results_dir, args.output, args.stats_env)
    if args.mode == "check":
        return check(args.results_dir, args.reference)
    parser.error(f"unknown mode {args.mode}")
    return 2


if __name__ == "__main__":
    sys.exit(main())
