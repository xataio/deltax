#!/usr/bin/env python3
"""Pull the comparison results rtabench.com inlines and save them one JSON per
system under ~/src/rtabench/<subdir>/results/, so generate-report.py's tree
mode can pick them up alongside our own pg_deltax results.

rtabench.com ships ~20 entries (system × machine). We save everything at a
machine we care about (c6a.4xlarge by default, override with `--machine`) so
that all JSONs in the tree are comparable on the same hardware.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import urllib.request
from pathlib import Path


URL = "https://rtabench.com/"

# Map rtabench.com `system` → existing subdir name under ~/src/rtabench/.
SYSTEM_TO_SUBDIR = {
    "ClickHouse": "clickhouse",
    "ClickHouse Cloud (aws)": "clickhouse-cloud",
    "Doris": "doris",
    "DuckDB": "duckdb",
    "MongoDB": "mongodb",
    "MySQL": "mysql",
    "Postgres": "postgres",
    "Timescale Cloud": "timescale-cloud",
    "TimescaleDB": "timescaledb",
}


def fetch() -> list[dict]:
    req = urllib.request.Request(URL, headers={"User-Agent": "pg_deltax-rtabench-fetch/1"})
    with urllib.request.urlopen(req, timeout=30) as r:
        src = r.read().decode("utf-8", errors="replace")
    m = re.search(r"const\s+data\s*=\s*\[", src)
    if not m:
        raise SystemExit("rtabench.com page source does not contain `const data = [...]`")
    arr, _ = json.JSONDecoder().raw_decode(src[m.end() - 1 :])
    return arr


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--rtabench-root", default=str(Path.home() / "src" / "rtabench"),
                    help="Path to rtabench checkout (default: ~/src/rtabench)")
    ap.add_argument("--machine", default="c6a.4xlarge",
                    help="Machine prefix to keep (default: c6a.4xlarge). "
                         "Pass an empty string to keep all machine variants.")
    args = ap.parse_args()

    root = Path(args.rtabench_root).expanduser().resolve()
    if not root.is_dir():
        print(f"error: {root} is not a directory", file=sys.stderr)
        return 1

    entries = fetch()
    print(f"fetched {len(entries)} entries from {URL}")

    kept = 0
    for e in entries:
        system = e.get("system", "")
        machine = e.get("machine", "")
        if args.machine and not machine.startswith(args.machine):
            continue
        subdir_name = SYSTEM_TO_SUBDIR.get(system)
        if subdir_name is None:
            print(f"  skip  [{system}] — no mapping to a rtabench subdir", file=sys.stderr)
            continue
        target_dir = root / subdir_name / "results"
        target_dir.mkdir(parents=True, exist_ok=True)
        # Filename uses the first token of machine so naming is stable and
        # matches our own results/c6a.4xlarge.json pattern.
        mach_token = machine.split(",")[0].strip().split()[0] if machine else "unknown"
        out = target_dir / f"{mach_token}.json"
        out.write_text(json.dumps(e, indent=2) + "\n")
        kept += 1
        print(f"  wrote {out.relative_to(root)}  ({system}, {machine})")

    print(f"saved {kept} JSONs under {root}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
