#!/usr/bin/env python3
"""Generate a single-file HTML report from rtabench result JSON(s).

Reads `results/pg_deltax.json` (and any other `results/*.json` as comparison
systems, and `results/history/*/pg_deltax.json` for a commit-trend column).
Writes `results/index.html`.

No external deps; open the output directly in a browser.
"""

from __future__ import annotations

import html
import json
import sys
from pathlib import Path


# Two modes, auto-detected from the first CLI arg:
#
#   (a) "flat": `<base>/results/` holds all JSONs. Output goes to
#       `<base>/results/index.html`. This is the default for our in-repo
#       `rtabench/results/`.
#
#   (b) "tree": `<base>/<system>/results/*.json` (ClickBench-style). Used
#       when pointing at `~/src/rtabench/` — each sibling dir (postgres/,
#       timescaledb/, pg_deltax/, …) contributes its own results JSON, and
#       we produce a single comparison `<base>/index.html` at the root.
#
# No arg → flat, rooted at the script's directory.
if len(sys.argv) > 1:
    BASE_DIR = Path(sys.argv[1]).expanduser().resolve()
else:
    BASE_DIR = Path(__file__).parent

QUERIES_DIR = Path(__file__).parent / "queries"

TREE_MODE = not (BASE_DIR / "results").is_dir()
if TREE_MODE:
    RESULTS_DIR = BASE_DIR  # each subdir has its own results/
    HISTORY_DIR = BASE_DIR / "pg_deltax" / "results" / "history"
    OUTPUT = BASE_DIR / "index.html"
else:
    RESULTS_DIR = BASE_DIR / "results"
    HISTORY_DIR = RESULTS_DIR / "history"
    OUTPUT = RESULTS_DIR / "index.html"


def load(path: Path) -> dict | None:
    try:
        with path.open() as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return None


def query_names() -> list[str]:
    return sorted(p.stem for p in QUERIES_DIR.glob("*.sql"))


def _positive(vals: list) -> list[float]:
    """Only keep numeric times > 0. rtabench.com encodes errors/timeouts as -1;
    treat those as missing so they don't display as negative ms or pollute min."""
    return [float(x) for x in vals if isinstance(x, (int, float)) and x > 0]


def cell_min(run: list[float | None]) -> float | None:
    vals = _positive(run)
    return min(vals) if vals else None


def cell_warm(run: list[float | None]) -> float | None:
    # Warm = min of runs 2+3 (index 1 and 2); falls back to run 3 alone if 2 missing.
    tail = _positive(run[1:3])
    return min(tail) if tail else None


def fmt_time(v: float | None) -> str:
    if v is None:
        return '<span class="null">—</span>'
    if v < 0.001:
        return f'<span class="fast">{v*1000:.2f} ms</span>'
    if v < 1:
        return f'{v*1000:.1f} ms'
    return f'<b>{v:.3f} s</b>'


def fmt_size(b: int) -> str:
    if b <= 0:
        return "—"
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if b < 1024:
            return f"{b:.1f} {unit}"
        b /= 1024
    return f"{b:.1f} PiB"


def fmt_time_sec(s: int) -> str:
    if s <= 0:
        return "—"
    h, rem = divmod(s, 3600)
    m, s = divmod(rem, 60)
    parts = []
    if h:
        parts.append(f"{h}h")
    if m:
        parts.append(f"{m}m")
    parts.append(f"{s}s")
    return " ".join(parts)


def render(primary: dict, comparisons: list[tuple[str, dict]], history: list[tuple[str, dict]]) -> str:
    qnames = query_names()
    n_q = len(primary.get("result", []))
    tags = ", ".join(primary.get("tags", []))

    # Header block
    header_rows = [
        ("System", html.escape(primary.get("system", "?"))),
        ("Date", html.escape(primary.get("date", "?"))),
        ("Machine", html.escape(primary.get("machine", "?"))),
        ("Tags", html.escape(tags) if tags else "—"),
        ("Load time", fmt_time_sec(int(primary.get("load_time", 0)))),
        ("Data size", fmt_size(int(primary.get("data_size", 0)))),
        ("Queries", str(n_q)),
    ]

    # Primary results table
    rows_html: list[str] = []
    totals_cold, totals_warm = 0.0, 0.0
    count_cold, count_warm = 0, 0

    extra_systems = [name for name, _ in comparisons]

    for i, q in enumerate(primary["result"]):
        name = qnames[i] if i < len(qnames) else f"q{i:04d}"
        cold = q[0] if q and isinstance(q[0], (int, float)) else None
        warm = cell_warm(q)
        mn = cell_min(q)
        if cold is not None:
            totals_cold += cold
            count_cold += 1
        if warm is not None:
            totals_warm += warm
            count_warm += 1

        cells = [
            f"<td class=\"idx\">Q{i:02d}</td>",
            f"<td class=\"qname\">{html.escape(name)}</td>",
            f"<td>{fmt_time(cold)}</td>",
            f"<td>{fmt_time(warm)}</td>",
            f"<td>{fmt_time(mn)}</td>",
        ]
        for _, other in comparisons:
            orow = other.get("result", [])
            ov = cell_min(orow[i]) if i < len(orow) and orow[i] else None
            ratio = ""
            if ov is not None and mn is not None and mn > 0:
                r = ov / mn
                cls = "faster" if r > 1.1 else ("slower" if r < 0.9 else "neutral")
                ratio = f' <span class="ratio {cls}">×{r:.1f}</span>'
            cells.append(f"<td>{fmt_time(ov)}{ratio}</td>")
        rows_html.append("<tr>" + "".join(cells) + "</tr>")

    extra_headers = "".join(
        f'<th>{html.escape(name)}</th>' for name in extra_systems
    )

    # Commit-trend table
    history_html = ""
    if history:
        hist_rows: list[str] = []
        for name, data in history:
            warm_total = 0.0
            cnt = 0
            for q in data.get("result", []):
                w = cell_warm(q)
                if w is not None:
                    warm_total += w
                    cnt += 1
            avg_warm = warm_total / cnt if cnt else 0.0
            hist_rows.append(
                f"<tr><td>{html.escape(name)}</td>"
                f"<td>{fmt_time_sec(int(data.get('load_time', 0)))}</td>"
                f"<td>{fmt_size(int(data.get('data_size', 0)))}</td>"
                f"<td>{cnt}</td>"
                f"<td>{warm_total:.3f} s</td>"
                f"<td>{avg_warm*1000:.1f} ms</td></tr>"
            )
        history_html = (
            "<h2>History (warm sum = sum of min-of-warm-runs across queries)</h2>"
            "<table><thead><tr>"
            "<th>Snapshot</th><th>Load</th><th>Size</th><th>#Q</th>"
            "<th>Warm sum</th><th>Warm avg</th>"
            "</tr></thead><tbody>"
            + "".join(hist_rows)
            + "</tbody></table>"
        )

    summary_warm = f"{totals_warm:.3f} s total, {totals_warm/count_warm*1000:.1f} ms avg" if count_warm else "—"
    summary_cold = f"{totals_cold:.3f} s total, {totals_cold/count_cold*1000:.1f} ms avg" if count_cold else "—"

    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>rtabench — {html.escape(primary.get('system', 'results'))}</title>
<style>
  body {{ font: 14px/1.5 -apple-system, system-ui, sans-serif; margin: 2em; color: #222; max-width: 1100px; }}
  h1 {{ margin: 0 0 .25em; font-size: 1.5em; }}
  h2 {{ font-size: 1.1em; margin-top: 2em; }}
  .meta {{ display: grid; grid-template-columns: max-content 1fr; gap: 4px 1em; margin: 1em 0; }}
  .meta dt {{ color: #666; }}
  .summary {{ background: #f4f4f4; padding: .75em 1em; border-radius: 4px; margin: 1em 0; }}
  table {{ border-collapse: collapse; width: 100%; font-variant-numeric: tabular-nums; }}
  th, td {{ text-align: right; padding: 4px 10px; border-bottom: 1px solid #eee; }}
  th:nth-child(1), th:nth-child(2),
  td:nth-child(1), td:nth-child(2) {{ text-align: left; }}
  th {{ background: #fafafa; font-weight: 600; border-bottom: 2px solid #ddd; }}
  .idx {{ color: #888; font-family: ui-monospace, Menlo, monospace; }}
  .qname {{ font-family: ui-monospace, Menlo, monospace; font-size: .92em; }}
  .null {{ color: #bbb; }}
  .fast {{ color: #2a7; }}
  .ratio {{ margin-left: .3em; font-size: .85em; }}
  .ratio.faster {{ color: #070; }}
  .ratio.slower {{ color: #b00; }}
  .ratio.neutral {{ color: #888; }}
  .footer {{ color: #888; font-size: .85em; margin-top: 3em; }}
</style>
</head>
<body>

<h1>RTABench results — {html.escape(primary.get('system', 'pg_deltax'))}</h1>

<dl class="meta">
{"".join(f'<dt>{html.escape(k)}</dt><dd>{v}</dd>' for k, v in header_rows)}
</dl>

<div class="summary">
<b>Cold (1st run):</b> {summary_cold}<br>
<b>Warm (best of runs 2–3):</b> {summary_warm}
</div>

<table>
<thead><tr>
<th>#</th><th>Query</th><th>Cold</th><th>Warm</th><th>Min</th>{extra_headers}
</tr></thead>
<tbody>
{"".join(rows_html)}
</tbody>
</table>

{history_html}

<p class="footer">
Generated by rtabench/generate-report.py ·
Cold = run 1 · Warm = min(run 2, run 3) · Min = min of all 3 runs ·
Competitor cells show <b>their min time</b> and a ratio vs. ours —
<span class="ratio faster">×&gt;1</span> means we are faster,
<span class="ratio slower">×&lt;1</span> means the competitor is faster.
Competitor data is pulled from rtabench.com by <code>make fetch-competitors</code>.
</p>

</body>
</html>
"""


def collect_tree_results() -> tuple[dict | None, Path | None, list[tuple[str, dict]]]:
    """In tree mode (ClickBench-style root), each sibling dir under BASE_DIR has
    its own results/*.json. pg_deltax is the primary; everything else is a
    comparison column. Subdirs without a results/ are silently skipped."""
    primary = None
    primary_path = None
    comparisons: list[tuple[str, dict]] = []
    for sub in sorted(p for p in BASE_DIR.iterdir() if p.is_dir()):
        res_dir = sub / "results"
        if not res_dir.is_dir():
            continue
        jsons = sorted(res_dir.glob("*.json"))
        if not jsons:
            continue
        # Prefer pg_deltax.json if present; otherwise the first json (e.g. c6a.4xlarge.json).
        chosen = next((p for p in jsons if p.name == "pg_deltax.json"), jsons[0])
        data = load(chosen)
        if not data:
            continue
        label = data.get("system") or sub.name
        if sub.name == "pg_deltax":
            primary = data
            primary_path = chosen
        else:
            comparisons.append((label, data))
    return primary, primary_path, comparisons


def collect_flat_results() -> tuple[dict | None, Path | None, list[tuple[str, dict]]]:
    # Prefer `pg_deltax.json` as primary (our local layout). Fall back to any
    # machine-named JSON (rtabench-style mirror uses `c6a.4xlarge.json` etc.).
    candidates = [RESULTS_DIR / "pg_deltax.json"]
    candidates += sorted(p for p in RESULTS_DIR.glob("*.json") if p.name != "pg_deltax.json")
    primary = None
    primary_path = None
    for p in candidates:
        data = load(p)
        if data:
            primary = data
            primary_path = p
            break
    comparisons: list[tuple[str, dict]] = []
    if primary_path is not None:
        for p in sorted(RESULTS_DIR.glob("*.json")):
            if p == primary_path:
                continue
            data = load(p)
            if data:
                comparisons.append((p.stem, data))
    return primary, primary_path, comparisons


def main() -> int:
    if TREE_MODE:
        primary, primary_path, comparisons = collect_tree_results()
        if not primary:
            print(
                f"error: no pg_deltax/results/*.json under {BASE_DIR} — "
                "`make bench` first, then re-run `make report`",
                file=sys.stderr,
            )
            return 1
    else:
        primary, primary_path, comparisons = collect_flat_results()
        if not primary:
            print(
                f"error: no results JSON found under {RESULTS_DIR} — "
                "run `make bench EC2=<ip>` first",
                file=sys.stderr,
            )
            return 1

    # History: one row per archived commit snapshot
    history: list[tuple[str, dict]] = []
    if HISTORY_DIR.is_dir():
        for snap_dir in sorted(HISTORY_DIR.iterdir()):
            snap_json = snap_dir / "pg_deltax.json"
            data = load(snap_json)
            if data:
                history.append((snap_dir.name, data))

    html_out = render(primary, comparisons, history)
    OUTPUT.write_text(html_out)
    print(f"wrote {OUTPUT}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
