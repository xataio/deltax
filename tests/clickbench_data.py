"""Shared ClickBench data loading utilities.

Constants, download helpers, parquet-to-PostgreSQL loading, and a generic
query runner used by both the pg_deltax and TimescaleDB benchmarks.
"""

import io
import os
import statistics
import time
import urllib.request
from pathlib import Path

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DATA_DIR = Path(__file__).parent / ".data"
PARQUET_URL = "https://datasets.clickhouse.com/hits_compatible/athena_partitioned/hits_{idx}.parquet"
NUM_FILES = int(os.environ.get("CLICKBENCH_FILES", "1"))
WARMUP_RUNS = 1
TIMED_RUNS = 3

# ClickBench schema:
#   - EventTime / ClientEventTime / LocalEventTime as TIMESTAMPTZ
#   - All NOT NULL constraints kept
CREATE_TABLE_SQL = """\
CREATE TABLE hits (
    WatchID BIGINT NOT NULL,
    JavaEnable SMALLINT NOT NULL,
    Title TEXT NOT NULL,
    GoodEvent SMALLINT NOT NULL,
    EventTime TIMESTAMPTZ NOT NULL,
    EventDate DATE NOT NULL,
    CounterID INTEGER NOT NULL,
    ClientIP INTEGER NOT NULL,
    RegionID INTEGER NOT NULL,
    UserID BIGINT NOT NULL,
    CounterClass SMALLINT NOT NULL,
    OS SMALLINT NOT NULL,
    UserAgent SMALLINT NOT NULL,
    URL TEXT NOT NULL,
    Referer TEXT NOT NULL,
    IsRefresh SMALLINT NOT NULL,
    RefererCategoryID SMALLINT NOT NULL,
    RefererRegionID INTEGER NOT NULL,
    URLCategoryID SMALLINT NOT NULL,
    URLRegionID INTEGER NOT NULL,
    ResolutionWidth SMALLINT NOT NULL,
    ResolutionHeight SMALLINT NOT NULL,
    ResolutionDepth SMALLINT NOT NULL,
    FlashMajor SMALLINT NOT NULL,
    FlashMinor SMALLINT NOT NULL,
    FlashMinor2 TEXT NOT NULL,
    NetMajor SMALLINT NOT NULL,
    NetMinor SMALLINT NOT NULL,
    UserAgentMajor SMALLINT NOT NULL,
    UserAgentMinor VARCHAR(255) NOT NULL,
    CookieEnable SMALLINT NOT NULL,
    JavascriptEnable SMALLINT NOT NULL,
    IsMobile SMALLINT NOT NULL,
    MobilePhone SMALLINT NOT NULL,
    MobilePhoneModel TEXT NOT NULL,
    Params TEXT NOT NULL,
    IPNetworkID INTEGER NOT NULL,
    TraficSourceID SMALLINT NOT NULL,
    SearchEngineID SMALLINT NOT NULL,
    SearchPhrase TEXT NOT NULL,
    AdvEngineID SMALLINT NOT NULL,
    IsArtifical SMALLINT NOT NULL,
    WindowClientWidth SMALLINT NOT NULL,
    WindowClientHeight SMALLINT NOT NULL,
    ClientTimeZone SMALLINT NOT NULL,
    ClientEventTime TIMESTAMPTZ NOT NULL,
    SilverlightVersion1 SMALLINT NOT NULL,
    SilverlightVersion2 SMALLINT NOT NULL,
    SilverlightVersion3 INTEGER NOT NULL,
    SilverlightVersion4 SMALLINT NOT NULL,
    PageCharset TEXT NOT NULL,
    CodeVersion INTEGER NOT NULL,
    IsLink SMALLINT NOT NULL,
    IsDownload SMALLINT NOT NULL,
    IsNotBounce SMALLINT NOT NULL,
    FUniqID BIGINT NOT NULL,
    OriginalURL TEXT NOT NULL,
    HID INTEGER NOT NULL,
    IsOldCounter SMALLINT NOT NULL,
    IsEvent SMALLINT NOT NULL,
    IsParameter SMALLINT NOT NULL,
    DontCountHits SMALLINT NOT NULL,
    WithHash SMALLINT NOT NULL,
    HitColor CHAR NOT NULL,
    LocalEventTime TIMESTAMPTZ NOT NULL,
    Age SMALLINT NOT NULL,
    Sex SMALLINT NOT NULL,
    Income SMALLINT NOT NULL,
    Interests SMALLINT NOT NULL,
    Robotness SMALLINT NOT NULL,
    RemoteIP INTEGER NOT NULL,
    WindowName INTEGER NOT NULL,
    OpenerName INTEGER NOT NULL,
    HistoryLength SMALLINT NOT NULL,
    BrowserLanguage TEXT NOT NULL,
    BrowserCountry TEXT NOT NULL,
    SocialNetwork TEXT NOT NULL,
    SocialAction TEXT NOT NULL,
    HTTPError SMALLINT NOT NULL,
    SendTiming INTEGER NOT NULL,
    DNSTiming INTEGER NOT NULL,
    ConnectTiming INTEGER NOT NULL,
    ResponseStartTiming INTEGER NOT NULL,
    ResponseEndTiming INTEGER NOT NULL,
    FetchTiming INTEGER NOT NULL,
    SocialSourceNetworkID SMALLINT NOT NULL,
    SocialSourcePage TEXT NOT NULL,
    ParamPrice BIGINT NOT NULL,
    ParamOrderID TEXT NOT NULL,
    ParamCurrency TEXT NOT NULL,
    ParamCurrencyID SMALLINT NOT NULL,
    OpenstatServiceName TEXT NOT NULL,
    OpenstatCampaignID TEXT NOT NULL,
    OpenstatAdID TEXT NOT NULL,
    OpenstatSourceID TEXT NOT NULL,
    UTMSource TEXT NOT NULL,
    UTMMedium TEXT NOT NULL,
    UTMCampaign TEXT NOT NULL,
    UTMContent TEXT NOT NULL,
    UTMTerm TEXT NOT NULL,
    FromTag TEXT NOT NULL,
    HasGCLID SMALLINT NOT NULL,
    RefererHash BIGINT NOT NULL,
    URLHash BIGINT NOT NULL,
    CLID INTEGER NOT NULL
)"""

# Column names in order, matching the schema above
COLUMN_NAMES = [
    "WatchID", "JavaEnable", "Title", "GoodEvent", "EventTime", "EventDate",
    "CounterID", "ClientIP", "RegionID", "UserID", "CounterClass", "OS",
    "UserAgent", "URL", "Referer", "IsRefresh", "RefererCategoryID",
    "RefererRegionID", "URLCategoryID", "URLRegionID", "ResolutionWidth",
    "ResolutionHeight", "ResolutionDepth", "FlashMajor", "FlashMinor",
    "FlashMinor2", "NetMajor", "NetMinor", "UserAgentMajor", "UserAgentMinor",
    "CookieEnable", "JavascriptEnable", "IsMobile", "MobilePhone",
    "MobilePhoneModel", "Params", "IPNetworkID", "TraficSourceID",
    "SearchEngineID", "SearchPhrase", "AdvEngineID", "IsArtifical",
    "WindowClientWidth", "WindowClientHeight", "ClientTimeZone",
    "ClientEventTime", "SilverlightVersion1", "SilverlightVersion2",
    "SilverlightVersion3", "SilverlightVersion4", "PageCharset", "CodeVersion",
    "IsLink", "IsDownload", "IsNotBounce", "FUniqID", "OriginalURL", "HID",
    "IsOldCounter", "IsEvent", "IsParameter", "DontCountHits", "WithHash",
    "HitColor", "LocalEventTime", "Age", "Sex", "Income", "Interests",
    "Robotness", "RemoteIP", "WindowName", "OpenerName", "HistoryLength",
    "BrowserLanguage", "BrowserCountry", "SocialNetwork", "SocialAction",
    "HTTPError", "SendTiming", "DNSTiming", "ConnectTiming",
    "ResponseStartTiming", "ResponseEndTiming", "FetchTiming",
    "SocialSourceNetworkID", "SocialSourcePage", "ParamPrice", "ParamOrderID",
    "ParamCurrency", "ParamCurrencyID", "OpenstatServiceName",
    "OpenstatCampaignID", "OpenstatAdID", "OpenstatSourceID", "UTMSource",
    "UTMMedium", "UTMCampaign", "UTMContent", "UTMTerm", "FromTag",
    "HasGCLID", "RefererHash", "URLHash", "CLID",
]


# ---------------------------------------------------------------------------
# Data download & loading
# ---------------------------------------------------------------------------

def download_parquet(idx: int) -> Path:
    """Download a single parquet file, caching in tests/.data/."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    dest = DATA_DIR / f"hits_{idx}.parquet"
    if dest.exists():
        print(f"  [cached] {dest.name}")
        return dest

    url = PARQUET_URL.format(idx=idx)
    print(f"  Downloading {url} ...")
    req = urllib.request.Request(url, headers={"User-Agent": "pg_deltax-bench/1.0"})
    with urllib.request.urlopen(req) as resp, open(dest, "wb") as f:
        while True:
            chunk = resp.read(1 << 20)  # 1 MB chunks
            if not chunk:
                break
            f.write(chunk)
    print(f"  Saved {dest.name} ({dest.stat().st_size / 1e6:.1f} MB)")
    return dest


def _convert_parquet_table(table):
    """Convert parquet table columns to PostgreSQL-compatible types.

    - int64 epoch-seconds timestamps -> timestamp strings
    - uint16 epoch-days dates -> date strings
    - binary text -> utf-8 strings
    """
    import pyarrow as pa
    import pyarrow.compute as pc

    EPOCH_SEC_COLS = {"EventTime", "ClientEventTime", "LocalEventTime"}
    EPOCH_DAY_COLS = {"EventDate"}

    new_columns = []
    for i, name in enumerate(table.column_names):
        col = table.column(i)
        if name in EPOCH_SEC_COLS:
            ts_array = col.cast(pa.timestamp("s", tz="UTC"))
            new_columns.append(ts_array.cast(pa.string()))
        elif name in EPOCH_DAY_COLS:
            date_array = col.cast(pa.int32()).cast(pa.date32())
            new_columns.append(date_array.cast(pa.string()))
        elif pa.types.is_binary(col.type):
            new_columns.append(col.cast(pa.string()))
        else:
            new_columns.append(col)

    return pa.table(new_columns, names=table.column_names)


def load_parquet_file(conn, parquet_path: Path):
    """Load a single parquet file into the hits table using pyarrow CSV + COPY."""
    import pyarrow.csv as pcsv
    import pyarrow.parquet as pq

    table = pq.read_table(parquet_path)
    table = _convert_parquet_table(table)

    buf = io.BytesIO()
    pcsv.write_csv(table, buf, write_options=pcsv.WriteOptions(include_header=False))
    buf.seek(0)

    col_list = ", ".join(c.lower() for c in COLUMN_NAMES)
    with conn.cursor() as cur:
        with cur.copy(f"COPY hits ({col_list}) FROM STDIN WITH (FORMAT csv)") as copy:
            while True:
                chunk = buf.read(1 << 20)
                if not chunk:
                    break
                copy.write(chunk)

    conn.commit()


def load_parquet_files(conn, n: int):
    """Download and load n parquet files into the hits table."""
    for idx in range(n):
        path = download_parquet(idx)
        print(f"  Loading {path.name} into PostgreSQL ...")
        t0 = time.monotonic()
        load_parquet_file(conn, path)
        elapsed = time.monotonic() - t0
        print(f"  Loaded {path.name} in {elapsed:.1f}s")


# ---------------------------------------------------------------------------
# Query benchmarking
# ---------------------------------------------------------------------------

BENCH_RESULTS_DIR = Path(__file__).parent / ".bench_results"


def _get_git_commit_short() -> str:
    """Return short git commit hash, or 'unknown' on failure."""
    import subprocess
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=Path(__file__).parent.parent,
            stderr=subprocess.DEVNULL,
        ).decode().strip()
    except Exception:
        return "unknown"


def save_bench_results(system_name: str, data: dict):
    """Save benchmark results to a JSON file for cross-system comparison.

    Also archives a timestamped copy under .bench_results/history/ with the
    format: YYYYMMDD_HHMMSS_<commit>/<system_name>.json
    """
    import json
    from datetime import datetime

    BENCH_RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    # Save latest (used by bench_compare.py)
    dest = BENCH_RESULTS_DIR / f"{system_name}.json"
    with open(dest, "w") as f:
        json.dump(data, f, indent=2)
    print(f"\n  Results saved to {dest}")

    # Archive to history directory: .bench_results/history/<timestamp>_<commit>/
    commit = _get_git_commit_short()
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    history_dir = BENCH_RESULTS_DIR / "history" / f"{ts}_{commit}"
    history_dir.mkdir(parents=True, exist_ok=True)
    history_dest = history_dir / f"{system_name}.json"
    with open(history_dest, "w") as f:
        json.dump(data, f, indent=2)
    print(f"  Archived to {history_dest}")


def query_results_to_dict(results: dict) -> dict:
    """Convert {qid: (median_ms, rows)} to {qid: median_ms} for JSON serialization.

    Converts float("inf") to None since inf is not valid JSON.
    """
    out = {}
    for qid, (median_ms, _rows) in results.items():
        out[qid] = None if median_ms == float("inf") else median_ms
    return out


def run_queries(conn, queries, label=""):
    """Run each query with warmup + timed runs.

    Returns {qid: (median_ms, result_rows)} where result_rows is the list of
    tuples from the last successful run (used for validation).
    """
    results = {}
    for qid, desc, sql in queries:
        # Warmup
        for _ in range(WARMUP_RUNS):
            try:
                conn.execute(sql).fetchall()
            except Exception:
                conn.rollback()

        # Timed runs
        timings = []
        last_rows = None
        last_error = None
        for _ in range(TIMED_RUNS):
            t0 = time.monotonic()
            try:
                rows = conn.execute(sql).fetchall()
                elapsed = (time.monotonic() - t0) * 1000  # ms
                timings.append(elapsed)
                last_rows = rows
            except Exception as e:
                conn.rollback()
                timings.append(float("inf"))
                last_error = e

        median = statistics.median(timings)
        results[qid] = (median, last_rows)

        status = f"{median:.1f}ms" if median != float("inf") else "ERROR"
        print(f"  [{label}] {qid} ({desc}): {status}")
        if last_error is not None:
            print(f"    ERROR: {last_error}")

    return results
