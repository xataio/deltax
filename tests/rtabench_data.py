"""Shared RTABench data loading utilities for the local Docker bench.

Streams the five CSVs from rtadatasets.timescale.com, slices by order_id
membership (to fit in a sub-GB local dataset), and loads the same rows
into two variants of `order_events` — a plain PG table and a pg_deltax
compressed table — so the bench can measure deltax vs plain PG head to
head and diff result sets for correctness.

Caching: CSVs + slice outputs live under `tests/.data/rtabench/`; re-runs
skip download and slicing when the cached files are non-empty.
"""

from __future__ import annotations

import csv
import gzip
import io
import os
import time
import urllib.request
from pathlib import Path


DATA_DIR = Path(__file__).parent / ".data" / "rtabench"
BASE_URL = "https://rtadatasets.timescale.com"

# Number of rows from `orders.csv` to keep. Referentially, `order_items` and
# `order_events` are then filtered to only those order_ids. Default is ~1/40
# of the full dataset so it fits on a dev laptop in a few minutes.
RTABENCH_ORDERS = int(os.environ.get("RTABENCH_ORDERS", "250000"))

WARMUP_RUNS = 1
TIMED_RUNS = 3

# pg_deltax configuration for the compressed `order_events` table.
#  - `mock_now` at 2024-01-01 + `1 month` interval × 15 ahead covers the full
#    year of data regardless of which orders got sampled.
#  - `order_by = [order_id, event_created]` matches the EC2 benchmark so the
#    segment-level min/max on `order_id` helps point-lookup queries.
MOCK_NOW = "2024-01-01 00:00:00+00"
# pg_deltax doesn't allow month-based intervals; use 30-day blocks. 15 ahead
# from 2024-01-01 covers ~15 months, safely spanning all of 2024.
PARTITION_INTERVAL = "30 days"
PARTITIONS_AHEAD = 15
SEGMENT_SIZE = 30000

# CSV columns, matching upstream files at rtadatasets.timescale.com.
CUSTOMERS_COLS = ["customer_id", "name", "birthday", "email", "address", "city", "zip", "state", "country"]
PRODUCTS_COLS = ["product_id", "name", "description", "category", "price", "stock"]
ORDERS_COLS = ["order_id", "customer_id", "created_at"]
ITEMS_COLS = ["order_id", "product_id", "amount"]
EVENTS_COLS = ["order_id", "counter", "event_created", "event_type",
               "satisfaction", "processor", "backup_processor", "event_payload"]

SCHEMA_SQL = """
CREATE TABLE customers (
    customer_id integer PRIMARY KEY,
    name        text,
    birthday    date,
    email       text,
    address     text,
    city        text,
    zip         text,
    state       text,
    country     text
);

CREATE TABLE products (
    product_id  integer PRIMARY KEY,
    name        text,
    description text,
    category    text,
    price       numeric(10,2),
    stock       int
);

CREATE TABLE orders (
    order_id    integer     PRIMARY KEY,
    customer_id integer     NOT NULL,
    created_at  timestamptz NOT NULL
);

CREATE INDEX ON orders (customer_id);

CREATE TABLE order_items (
    order_id   integer NOT NULL,
    product_id integer NOT NULL,
    amount     integer NOT NULL,
    PRIMARY KEY (order_id, product_id)
);

-- Two `order_events` variants with identical column layouts; the bench loads
-- the same sliced rows into both and diffs query results.
CREATE TABLE order_events_plain (
    order_id         integer     NOT NULL,
    counter          integer,
    event_created    timestamptz NOT NULL,
    event_type       text        NOT NULL,
    satisfaction     real        NOT NULL,
    processor        text        NOT NULL,
    backup_processor text,
    event_payload    jsonb
);

CREATE TABLE order_events (
    order_id         integer     NOT NULL,
    counter          integer,
    event_created    timestamptz NOT NULL,
    event_type       text        NOT NULL,
    satisfaction     real        NOT NULL,
    processor        text        NOT NULL,
    backup_processor text,
    event_payload    jsonb
);
"""


# ---------------------------------------------------------------------------
# Download + gunzip
# ---------------------------------------------------------------------------

def _download_and_gunzip(name: str) -> Path:
    """Return a path to `<name>.csv` in DATA_DIR, downloading + decompressing
    from the upstream .csv.gz if not already cached."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    csv_path = DATA_DIR / f"{name}.csv"
    if csv_path.exists() and csv_path.stat().st_size > 0:
        return csv_path

    url = f"{BASE_URL}/{name}.csv.gz"
    print(f"  Downloading {url} ...")
    t0 = time.monotonic()
    with urllib.request.urlopen(url) as resp, gzip.GzipFile(fileobj=resp) as gz, open(csv_path, "wb") as out:
        while True:
            chunk = gz.read(1 << 20)
            if not chunk:
                break
            out.write(chunk)
    dt = time.monotonic() - t0
    size_mb = csv_path.stat().st_size / 1e6
    print(f"    wrote {csv_path.name} ({size_mb:.1f} MB) in {dt:.1f}s")
    return csv_path


# ---------------------------------------------------------------------------
# Slicing
# ---------------------------------------------------------------------------

def _slice_dir(n_orders: int) -> Path:
    d = DATA_DIR / f"sliced_{n_orders}"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _slice_orders(src: Path, n_orders: int) -> tuple[Path, set[int]]:
    """Return (path_to_sliced_csv, order_id_set). Keeps the first n rows of
    orders.csv verbatim. Idempotent — reads from cache if slice exists."""
    out = _slice_dir(n_orders) / "orders.csv"
    if out.exists() and out.stat().st_size > 0:
        ids = set()
        with open(out, newline="") as f:
            for row in csv.reader(f):
                ids.add(int(row[0]))
        return out, ids

    print(f"  Slicing orders → first {n_orders:,} rows ...")
    ids: set[int] = set()
    with open(src, newline="") as fin, open(out, "w", newline="") as fout:
        reader = csv.reader(fin)
        writer = csv.writer(fout, lineterminator="\n")
        for row in reader:
            writer.writerow(row)
            ids.add(int(row[0]))
            if len(ids) >= n_orders:
                break
    return out, ids


def _filter_by_order_id(src: Path, dest: Path, order_ids: set[int]) -> int:
    """Copy src CSV → dest CSV keeping only rows whose first column (order_id)
    is in the given set. Returns the number of rows written. Idempotent."""
    if dest.exists() and dest.stat().st_size > 0:
        # Count rows in cached file
        with open(dest, "rb") as f:
            return sum(1 for _ in f)

    print(f"  Slicing {src.name} → {dest.name} ...")
    n = 0
    with open(src, newline="") as fin, open(dest, "w", newline="") as fout:
        reader = csv.reader(fin)
        writer = csv.writer(fout, lineterminator="\n")
        for row in reader:
            if int(row[0]) in order_ids:
                writer.writerow(row)
                n += 1
    return n


# ---------------------------------------------------------------------------
# COPY helpers
# ---------------------------------------------------------------------------

def _copy_csv(conn, table: str, csv_path: Path, *, deltax_compress: bool = False) -> float:
    """Stream a CSV file into `table` via server-side COPY. For deltax, use
    FORMAT deltax_compress_csv (single-threaded but CSV-aware legacy path)."""
    fmt = "deltax_compress_csv" if deltax_compress else "csv"
    sql = f"COPY {table} FROM STDIN WITH (FORMAT {fmt})"
    t0 = time.monotonic()
    with open(csv_path, "rb") as f, conn.cursor() as cur:
        with cur.copy(sql) as copy:
            while True:
                chunk = f.read(1 << 20)
                if not chunk:
                    break
                copy.write(chunk)
    conn.commit()
    return time.monotonic() - t0


# ---------------------------------------------------------------------------
# End-to-end orchestration
# ---------------------------------------------------------------------------

def _already_loaded(conn) -> bool:
    """Return True if the required tables already exist and `order_events`
    has rows — used by BENCH_PERSIST to skip re-loading on re-runs."""
    row = conn.execute(
        "SELECT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename='order_events')"
    ).fetchone()
    if not row or not row[0]:
        return False
    try:
        cnt = conn.execute("SELECT count(*) FROM order_events").fetchone()[0]
        return cnt > 0
    except Exception:
        conn.rollback()
        return False


def setup_schema(conn):
    """Run the CREATE TABLE statements + deltax init. Must be called before
    loading. Safe to call multiple times is NOT guaranteed — caller checks
    `_already_loaded` first."""
    conn.execute(f"SET pg_deltax.mock_now = '{MOCK_NOW}'")
    conn.execute(SCHEMA_SQL)
    conn.execute(
        f"SELECT deltax.deltax_create_table('order_events', 'event_created', "
        f"'{PARTITION_INTERVAL}'::interval, {PARTITIONS_AHEAD})"
    )
    # `event_payload->>'terminal'` is the only chain RTABench queries
    # touch (Q0/Q1/Q3/Q4/Q8/Q23). Pre-extract so the planner_hook walker
    # rewrites chains to synthetic-Var refs at query time. Mode is set
    # per-session by the bench fixture in `bench_rtabench.py`.
    conn.execute(
        "SELECT deltax.deltax_enable_compression('order_events', "
        f"order_by => ARRAY['order_id','event_created'], "
        f"segment_size => {SEGMENT_SIZE}, "
        "json_extract => '[{\"src\":\"event_payload\","
        "\"path\":[\"terminal\"],\"name\":\"x_terminal\",\"type\":\"text\"}]'::jsonb)"
    )
    conn.commit()


def load_all(conn, n_orders: int = RTABENCH_ORDERS) -> dict:
    """Download + slice + load everything. Idempotent: if `order_events` is
    already non-empty (BENCH_PERSIST path), returns early with cached counts.
    Returns {table: row_count} for reporting."""
    if _already_loaded(conn):
        print("\n--- Data already loaded (BENCH_PERSIST); skipping download/load ---")
        counts = {}
        for t in ("customers", "products", "orders", "order_items",
                  "order_events_plain", "order_events"):
            counts[t] = conn.execute(f"SELECT count(*) FROM {t}").fetchone()[0]
        return counts

    print("\n--- RTABench setup: schema + data load ---")
    setup_schema(conn)

    # 1. Download
    sources = {name: _download_and_gunzip(name) for name in
               ("customers", "products", "orders", "order_items", "order_events")}

    # 2. Slice orders → keep set of order_ids
    orders_slice, order_ids = _slice_orders(sources["orders"], n_orders)

    # 3. Filter items + events to orders in the set
    items_slice = _slice_dir(n_orders) / "order_items.csv"
    events_slice = _slice_dir(n_orders) / "order_events.csv"
    n_items = _filter_by_order_id(sources["order_items"], items_slice, order_ids)
    n_events = _filter_by_order_id(sources["order_events"], events_slice, order_ids)

    # 4. Load dimensions (full tables, small)
    print("\n  Loading dimension tables ...")
    _copy_csv(conn, "customers", sources["customers"])
    _copy_csv(conn, "products", sources["products"])

    # 5. Load sliced orders + items
    print("  Loading orders + order_items ...")
    _copy_csv(conn, "orders", orders_slice)
    _copy_csv(conn, "order_items", items_slice)

    # 6. Load order_events TWICE — once plain, once via direct backfill
    print("  Loading order_events_plain (plain PG) ...")
    t_plain = _copy_csv(conn, "order_events_plain", events_slice)
    print(f"    {n_events:,} rows in {t_plain:.1f}s")
    print("  Loading order_events (pg_deltax, direct backfill) ...")
    t_dx = _copy_csv(conn, "order_events", events_slice, deltax_compress=True)
    print(f"    {n_events:,} rows in {t_dx:.1f}s")

    # Vacuum / analyze so the planner has stats
    conn.rollback()
    conn.autocommit = True
    conn.execute("VACUUM ANALYZE")
    conn.autocommit = False

    counts = {
        "customers": conn.execute("SELECT count(*) FROM customers").fetchone()[0],
        "products": conn.execute("SELECT count(*) FROM products").fetchone()[0],
        "orders": conn.execute("SELECT count(*) FROM orders").fetchone()[0],
        "order_items": conn.execute("SELECT count(*) FROM order_items").fetchone()[0],
        "order_events_plain": conn.execute("SELECT count(*) FROM order_events_plain").fetchone()[0],
        "order_events": conn.execute("SELECT count(*) FROM order_events").fetchone()[0],
    }

    # Safety: default partition should be empty — if not, our mock_now /
    # partitions_ahead settings don't cover the data range.
    default_rows = conn.execute(
        "SELECT count(*) FROM order_events_default"
    ).fetchone()[0]
    assert default_rows == 0, (
        f"{default_rows} rows landed in order_events_default — widen "
        f"MOCK_NOW / PARTITIONS_AHEAD in rtabench_data.py"
    )
    return counts
