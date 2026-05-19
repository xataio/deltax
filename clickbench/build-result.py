#!/usr/bin/env python3
"""Build a ClickBench result JSON from a `bash benchmark.sh` log.

Consumes the structured stdout emitted by ClickBench's shared driver,
`lib/benchmark-common.sh`:

  Load time: <secs>
  [t1,t2,t3],            # one line per query, 43 total
  Data size: <bytes>
  Concurrent QPS: <N.NNN>
  Concurrent error ratio: <0.NNN>

Metadata (system, tags, proprietary, hardware, tuned, cluster_size) comes
from --template; date is stamped from the current UTC date; machine is
configurable via --machine (default c6a.4xlarge).

Output formatting matches the convention used by existing ClickBench
results files: one inline triplet per line under "result".
"""
import argparse
import json
import re
import sys
from datetime import datetime, timezone


def parse_log(log: str) -> dict:
    """Parse the structured stdout. Missing fields are simply absent from the
    returned dict (the caller can inherit from --base if so configured)."""
    out: dict = {}

    m = re.search(r"^Load time:\s*([0-9.]+)\s*$", log, re.M)
    if m:
        out["load_time"] = round(float(m.group(1)))

    triplets = []
    for line in log.splitlines():
        m = re.match(r"^\[([^\]]+)\],?\s*$", line.strip())
        if not m:
            continue
        parts = [x.strip() for x in m.group(1).split(",")]
        if len(parts) != 3:
            continue
        triplet = []
        for x in parts:
            if x == "null":
                triplet.append(None)
            else:
                try:
                    triplet.append(float(x))
                except ValueError:
                    triplet.append(None)
        triplets.append(triplet)
    if triplets:
        out["result"] = triplets

    m = re.search(r"^Data size:\s*([0-9]+)\s*$", log, re.M)
    if m:
        out["data_size"] = int(m.group(1))

    m = re.search(r"^Concurrent QPS:\s*([0-9.]+|null)\s*$", log, re.M)
    if m and m.group(1) != "null":
        out["concurrent_qps"] = float(m.group(1))

    m = re.search(r"^Concurrent error ratio:\s*([0-9.]+|null)\s*$", log, re.M)
    if m and m.group(1) != "null":
        out["concurrent_error_ratio"] = float(m.group(1))

    return out


def format_result(metadata: dict, parsed: dict, machine: str) -> str:
    fields = [
        ("system", metadata.get("system", "pg_deltax")),
        ("date", datetime.now(timezone.utc).strftime("%Y-%m-%d")),
        ("machine", machine),
        ("cluster_size", metadata.get("cluster_size", 1)),
        ("proprietary", metadata.get("proprietary", "no")),
        ("hardware", metadata.get("hardware", "cpu")),
        ("tuned", metadata.get("tuned", "no")),
        ("tags", metadata.get("tags", [])),
        ("load_time", parsed.get("load_time", 0)),
        ("data_size", parsed.get("data_size", 0)),
    ]
    if "concurrent_qps" in parsed:
        fields.append(("concurrent_qps", parsed["concurrent_qps"]))
    if "concurrent_error_ratio" in parsed:
        fields.append(("concurrent_error_ratio", parsed["concurrent_error_ratio"]))

    lines = ["{"]
    for k, v in fields:
        lines.append(f"    {json.dumps(k)}: {json.dumps(v)},")
    lines.append('    "result": [')
    triplets = parsed.get("result") or []
    for i, row in enumerate(triplets):
        cells = ", ".join("null" if x is None else format(x, "g") for x in row)
        comma = "," if i < len(triplets) - 1 else ""
        lines.append(f"        [{cells}]{comma}")
    lines.append("    ]")
    lines.append("}")
    return "\n".join(lines) + "\n"


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("log", help="Benchmark log file from `bash benchmark.sh`")
    p.add_argument("--template", required=True,
                   help="Path to template.json with metadata defaults")
    p.add_argument("--machine", default="c6a.4xlarge",
                   help="Machine label for the result entry")
    p.add_argument("--base",
                   help="Existing result JSON to inherit fields from when the "
                        "log doesn't contain them. Used by `make bench-concurrent` "
                        "to merge concurrent_qps into a result built by bench-full.")
    args = p.parse_args()

    with open(args.template) as f:
        metadata = json.load(f)
    with open(args.log) as f:
        log = f.read()
    parsed = parse_log(log)

    if args.base:
        with open(args.base) as f:
            base = json.load(f)
        for k in ("result", "load_time", "data_size",
                  "concurrent_qps", "concurrent_error_ratio"):
            if k not in parsed and k in base:
                parsed[k] = base[k]

    n = len(parsed.get("result") or [])
    if n == 0:
        sys.stderr.write(
            "error: 0 query triplets parsed and no --base to inherit from — the "
            "benchmark didn't produce any [t1,t2,t3] lines, so install/load/start "
            f"probably failed. Inspect {args.log} for the underlying error.\n"
        )
        sys.exit(1)
    if n != 43:
        sys.stderr.write(
            f"warning: result has {n} query triplets (expected 43)\n"
        )
    sys.stdout.write(format_result(metadata, parsed, args.machine))


if __name__ == "__main__":
    main()
