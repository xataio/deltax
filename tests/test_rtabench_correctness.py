"""RTABench-style correctness: run the same JOIN / filter / aggregate
queries against a pg_deltax-backed order_events and a plain Postgres
order_events_plain, asserting identical results.

This complements rtabench/ (EC2 performance workflow): the EC2 path measures
how fast the real ~171M-row dataset runs, this path verifies that the
answers pg_deltax returns on rtabench-shaped queries match plain Postgres
exactly on a tiny deterministic synthetic dataset.

The trick: both tables get the exact same rows, share the four dimension
tables (customers, products, orders, order_items), and every query is
written with an `{oe}` placeholder so we can run it twice — once against
`order_events` (deltax) and once against `order_events_plain` — and diff
the rows.
"""

import datetime as dt
import json
import random
from decimal import Decimal

import pytest


SEED = 20260422
# mock_now sits at the start of the data range; with premake=20 and a
# 1-day interval that gives 20 daily partitions forward — plenty of
# coverage for the 12 days of synthetic events below.
MOCK_NOW = "2024-05-01 00:00:00+00"
BASE_DAY = dt.datetime(2024, 5, 1, 0, 0, 0, tzinfo=dt.timezone.utc)
DAYS = 12

TERMINALS = ["Berlin", "Hamburg", "Munich", "Frankfurt", "Cologne"]
EVENT_TYPES = ["Created", "Departed", "Delivered", "Returned"]
PROCESSORS = ["proc-a", "proc-b", "proc-c"]
CATEGORIES = ["electronics", "books", "clothing", "toys"]
STATUSES = [
    ["Created"],
    ["Delayed", "Priority"],
    ["Delivered"],
    ["Returned"],
]

# Matches the real rtabench schema verbatim, including `event_payload jsonb`.
ORDER_EVENTS_COLS = """
    order_id integer NOT NULL,
    counter integer,
    event_created timestamptz NOT NULL,
    event_type text NOT NULL,
    satisfaction real NOT NULL,
    processor text NOT NULL,
    backup_processor text,
    event_payload jsonb
"""


def _create_schema(db):
    db.execute(
        "CREATE TABLE customers ("
        "customer_id integer PRIMARY KEY, name text, country text)"
    )
    db.execute(
        "CREATE TABLE products ("
        "product_id integer PRIMARY KEY, name text, category text, "
        "price numeric(10,2), stock int)"
    )
    db.execute(
        "CREATE TABLE orders ("
        "order_id integer PRIMARY KEY, customer_id integer NOT NULL, "
        "created_at timestamptz NOT NULL)"
    )
    db.execute(
        "CREATE TABLE order_items ("
        "order_id integer NOT NULL, product_id integer NOT NULL, "
        "amount integer NOT NULL, PRIMARY KEY (order_id, product_id))"
    )
    db.execute(f"CREATE TABLE order_events ({ORDER_EVENTS_COLS})")
    db.execute(f"CREATE TABLE order_events_plain ({ORDER_EVENTS_COLS})")
    db.commit()

    db.execute(
        "SELECT deltax.deltax_create_table('order_events', 'event_created', "
        "'1 day'::interval, 20)"
    )
    # Small segment_size exercises multi-segment paths on a tiny dataset.
    # `event_payload->>'terminal'` is the only JSONB chain RTABench
    # queries touch (Q0, Q1, Q3, Q4, Q8, Q23). Pre-extract it so the
    # planner_hook walker can rewrite chain Exprs to synthetic-Var refs
    # and DeltaXAgg picks those queries up directly.
    db.execute(
        "SELECT deltax.deltax_enable_compression('order_events', "
        "order_by => ARRAY['order_id','event_created'], segment_size => 100, "
        "json_extract => '[{\"src\":\"event_payload\","
        "\"path\":[\"terminal\"],\"name\":\"x_terminal\",\"type\":\"text\"}]'::jsonb)"
    )
    db.execute("SET pg_deltax.json_extract_mode = 'fields'")
    db.commit()


def _gen_data():
    rng = random.Random(SEED)
    customers = [
        (i, f"cust-{i}", rng.choice(["US", "DE", "FR", "UK", "IT"]))
        for i in range(1, 11)
    ]
    products = [
        (
            i,
            f"prod-{i}",
            rng.choice(CATEGORIES),
            round(rng.uniform(5, 500), 2),
            rng.randint(10, 500),
        )
        for i in range(1, 21)
    ]
    orders = []
    order_items = []
    for oid in range(1, 101):
        cid = rng.randint(1, 10)
        day = rng.randint(0, DAYS - 1)
        hour = rng.randint(0, 23)
        created_at = BASE_DAY + dt.timedelta(days=day, hours=hour)
        orders.append((oid, cid, created_at))
        n_items = rng.randint(1, 5)
        chosen = rng.sample(range(1, 21), n_items)
        for pid in chosen:
            order_items.append((oid, pid, rng.randint(1, 10)))

    events = []
    counter = 0
    for oid in range(1, 101):
        base_event = orders[oid - 1][2]
        n_ev = rng.randint(2, 20)
        for _ in range(n_ev):
            counter += 1
            # Constrain so events stay within the 12-day partition range.
            offset_hours = rng.randint(0, DAYS * 24 - 1) - (
                (base_event - BASE_DAY).total_seconds() // 3600
            )
            offset_hours = max(0, int(offset_hours))
            ts = base_event + dt.timedelta(
                hours=offset_hours, minutes=rng.randint(0, 59)
            )
            # Clamp to partition window end
            max_ts = BASE_DAY + dt.timedelta(days=DAYS) - dt.timedelta(minutes=1)
            if ts > max_ts:
                ts = max_ts
            payload = {
                "terminal": rng.choice(TERMINALS),
                "status": rng.choice(STATUSES),
                "lane": rng.randint(1, 8),
            }
            events.append(
                (
                    oid,
                    counter,
                    ts,
                    rng.choice(EVENT_TYPES),
                    round(rng.uniform(1, 5), 2),
                    rng.choice(PROCESSORS),
                    rng.choice(PROCESSORS + [None, None]),
                    json.dumps(payload),
                )
            )
    return customers, products, orders, order_items, events


def _escape_text(s):
    # PG TEXT COPY format: \b \f \n \r \t \v \\ are special.
    return (
        s.replace("\\", "\\\\")
        .replace("\t", "\\t")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
    )


def _to_text_copy(rows):
    lines = []
    for row in rows:
        parts = []
        for v in row:
            if v is None:
                parts.append("\\N")
            elif isinstance(v, dt.datetime):
                parts.append(v.isoformat())
            else:
                parts.append(_escape_text(str(v)))
        lines.append("\t".join(parts))
    return ("\n".join(lines) + "\n").encode()


def _copy(db, table, rows):
    with db.cursor() as cur:
        with cur.copy(f"COPY {table} FROM STDIN") as cp:
            cp.write(_to_text_copy(rows))


def _copy_events_deltax(db, rows):
    with db.cursor() as cur:
        with cur.copy(
            "COPY order_events FROM STDIN WITH (FORMAT deltax_compress)"
        ) as cp:
            cp.write(_to_text_copy(rows))


@pytest.fixture(scope="module")
def rtabench_data():
    return _gen_data()


@pytest.fixture()
def rtabench_db(db, rtabench_data):
    db.execute(f"SET pg_deltax.mock_now = '{MOCK_NOW}'")
    _create_schema(db)

    customers, products, orders, order_items, events = rtabench_data
    _copy(db, "customers", customers)
    _copy(db, "products", products)
    _copy(db, "orders", orders)
    _copy(db, "order_items", order_items)
    _copy(db, "order_events_plain", events)
    db.commit()

    _copy_events_deltax(db, events)
    db.commit()

    # Sanity: data must land in real partitions, not the default one.
    default_rows = db.execute(
        "SELECT count(*) FROM order_events_default"
    ).fetchone()[0]
    assert default_rows == 0, (
        f"{default_rows} event rows landed in order_events_default "
        "(outside partition range) — widen MOCK_NOW / premake."
    )

    # Row counts must match between the two tables — prereq for equality tests.
    plain_n = db.execute("SELECT count(*) FROM order_events_plain").fetchone()[0]
    deltax_n = db.execute("SELECT count(*) FROM order_events").fetchone()[0]
    assert plain_n == deltax_n, f"row count drift: plain={plain_n}, deltax={deltax_n}"
    assert plain_n > 100, f"dataset too small ({plain_n} rows) to be meaningful"

    db.execute("CREATE INDEX ON orders (customer_id)")
    db.execute(
        "ANALYZE customers, products, orders, "
        "order_items, order_events_plain"
    )
    db.commit()

    yield db


# Queries with {oe} placeholder — each is run twice (plain + deltax) and
# results compared row-by-row. Every query has a deterministic ORDER BY so
# the comparison is unambiguous.
QUERIES = [
    (
        "simple_agg",
        "SELECT count(*), "
        "sum(satisfaction)::numeric(20,4), "
        "avg(satisfaction)::numeric(20,4) "
        "FROM {oe}",
    ),
    (
        "terminal_hourly",
        """SELECT date_trunc('hour', event_created) AS bucket,
                  event_payload->>'terminal' AS terminal,
                  count(*) AS c
             FROM {oe}
            WHERE event_created >= '2024-05-03' AND event_created < '2024-05-08'
              AND event_type IN ('Created','Departed','Delivered')
            GROUP BY bucket, terminal
            ORDER BY bucket, terminal""",
    ),
    (
        "delayed_per_day",
        """SELECT date_trunc('day', event_created) AS day, count(*)
             FROM {oe}
            WHERE event_payload -> 'status' @> '["Delayed","Priority"]'::jsonb
            GROUP BY day
            ORDER BY day""",
    ),
    (
        "events_for_order_asc",
        """SELECT order_id, event_created, event_type
             FROM {oe}
            WHERE order_id = 42 AND event_created < '2024-05-15'
            ORDER BY event_created ASC
            LIMIT 5""",
    ),
    (
        "returned_per_day",
        """SELECT date_trunc('day', event_created) AS day, count(*)
             FROM {oe}
            WHERE event_type = 'Returned'
            GROUP BY day
            ORDER BY day""",
    ),
    (
        "customer_revenue",
        """SELECT c.customer_id, c.name,
                  sum(oi.amount * p.price)::numeric(20,2) AS revenue
             FROM customers c
             JOIN orders o USING (customer_id)
             JOIN order_items oi USING (order_id)
             JOIN products p USING (product_id)
             JOIN (SELECT DISTINCT order_id FROM {oe}
                    WHERE event_type = 'Delivered') d ON d.order_id = o.order_id
            WHERE o.created_at >= '2024-05-03' AND o.created_at < '2024-05-10'
            GROUP BY c.customer_id, c.name
            ORDER BY revenue DESC NULLS LAST, c.customer_id
            LIMIT 10""",
    ),
    (
        "category_performance",
        """SELECT p.category,
                  sum(oi.amount * p.price)::numeric(20,2) AS volume,
                  count(DISTINCT oi.order_id) AS orders
             FROM products p
             JOIN order_items oi USING (product_id)
             JOIN {oe} oe USING (order_id)
            WHERE oe.event_type = 'Delivered'
              AND oe.event_created >= '2024-05-03'
              AND oe.event_created < '2024-05-10'
            GROUP BY p.category
            ORDER BY volume DESC, p.category""",
    ),
    (
        "last_per_order_join",
        """SELECT DISTINCT ON (oe.order_id)
                  oe.order_id, oe.event_created, oe.event_type
             FROM {oe} oe
             JOIN orders o ON o.order_id = oe.order_id
            WHERE oe.order_id IN (5, 17, 42, 88)
            ORDER BY oe.order_id, oe.event_created DESC""",
    ),
]


def _cell_equal(a, b, *, abs_tol=1e-4, rel_tol=1e-6):
    if a is None or b is None:
        return a is None and b is None
    # Numeric: allow tiny float tolerance across Decimal/int/float.
    if isinstance(a, (int, float, Decimal)) and isinstance(b, (int, float, Decimal)):
        fa, fb = float(a), float(b)
        if fa == fb:
            return True
        return abs(fa - fb) <= max(abs_tol, rel_tol * max(abs(fa), abs(fb)))
    return a == b


def _rows_equal(a, b):
    if len(a) != len(b):
        return False
    return all(_cell_equal(x, y) for x, y in zip(a, b))


@pytest.mark.parametrize("name,sql", QUERIES, ids=[q[0] for q in QUERIES])
def test_rtabench_query_matches_plain_postgres(rtabench_db, name, sql):
    db = rtabench_db
    plain = db.execute(sql.format(oe="order_events_plain")).fetchall()
    deltax = db.execute(sql.format(oe="order_events")).fetchall()

    assert len(plain) > 0, (
        f"query '{name}' returned zero rows on plain — not a useful test"
    )
    assert len(plain) == len(deltax), (
        f"query '{name}': row count mismatch (plain={len(plain)}, "
        f"deltax={len(deltax)})"
    )

    mismatches = [
        (i, p, d)
        for i, (p, d) in enumerate(zip(plain, deltax))
        if not _rows_equal(p, d)
    ]
    if mismatches:
        msg = [f"query '{name}': {len(mismatches)} row(s) differ"]
        for i, p, d in mismatches[:5]:
            msg.append(f"  row {i}: plain={p!r}  deltax={d!r}")
        pytest.fail("\n".join(msg))


def test_jsonb_scan_low_cardinality_dict_encoding(db):
    """Regression test: jsonb payloads with low cardinality pick the Dictionary
    compression codec at flush time, whose decode path previously validated
    UTF-8 and panicked on binary jsonb bytes. Exercises the byte-level
    dictionary decode."""
    db.execute("SET pg_deltax.mock_now = '2024-01-01 00:00:00+00'")
    db.execute(
        "CREATE TABLE je (ts timestamptz NOT NULL, payload jsonb NOT NULL)"
    )
    db.execute("SELECT deltax.deltax_create_table('je', 'ts', '1 day'::interval, 5)")
    db.execute(
        "SELECT deltax.deltax_enable_compression('je', "
        "order_by => ARRAY['ts'], segment_size => 200)"
    )
    db.commit()

    # 500 rows, only 3 distinct jsonb values → cardinality is 3, well under
    # segment_size/2 → pg_deltax picks the Dictionary codec at flush time.
    payloads = [
        '{"terminal": "Berlin", "lane": 3}',
        '{"terminal": "Hamburg", "lane": 7}',
        '{"terminal": "Munich", "lane": 1}',
    ]
    rows = []
    for i in range(500):
        rows.append(f"2024-01-01T00:{i // 60:02d}:{i % 60:02d}+00\t{payloads[i % 3]}")
    data = "\n".join(rows) + "\n"
    with db.cursor() as cur:
        with cur.copy("COPY je FROM STDIN WITH (FORMAT deltax_compress)") as cp:
            cp.write(data.encode())
    db.commit()

    # Confirm dict codec really was chosen by checking the meta table exists
    # (any compression would create one; we can't easily read which codec was
    # chosen, but the scan test below is the real regression guard).
    db.execute("ANALYZE je")
    db.commit()

    # Sanity: SELECT the raw jsonb column back — exercises decompress +
    # varlena construction for the jsonb column, which is the UTF-8 panic
    # site for the Dictionary codec.
    rows = db.execute(
        "SELECT payload->>'terminal', count(*) FROM je GROUP BY 1 ORDER BY 1"
    ).fetchall()
    assert rows == [("Berlin", 167), ("Hamburg", 167), ("Munich", 166)], rows

    # Also exercise containment (uses full binary jsonb, not the text-extract path):
    cnt = db.execute(
        "SELECT count(*) FROM je WHERE payload @> '{\"lane\": 3}'"
    ).fetchone()[0]
    assert cnt == 167


def test_compression_actually_happened(rtabench_db):
    """Sanity check: the test exercises compressed segments, not a
    pass-through fallback. If this breaks, the other tests are
    comparing plain PG to plain PG and prove nothing."""
    db = rtabench_db
    compressed = db.execute(
        "SELECT count(*) FROM deltax.deltax_compression_stats('order_events') "
        "WHERE is_compressed = true AND row_count > 0"
    ).fetchone()[0]
    assert compressed > 0, (
        "no partitions were compressed — direct backfill did not run "
        "as expected; other correctness assertions may be vacuous"
    )


def test_desc_ordering_matches_plain(rtabench_db):
    """ORDER BY event_created DESC must match plain Postgres.

    Regression test for the PG17 bug where check_time_pathkey advertised
    a DESC pathkey but the decompress scan emits rows in ASC storage
    order — the planner would trust the advertisement and skip the sort,
    returning within-partition rows in the wrong direction. Fix: only
    advertise an ASC pathkey; planner adds a Sort for DESC queries."""
    db = rtabench_db
    sql = """
        SELECT order_id, event_created, event_type
          FROM {oe}
         WHERE order_id = 42 AND event_created < '2024-05-15'
         ORDER BY event_created DESC
         LIMIT 5
    """
    plain = db.execute(sql.format(oe="order_events_plain")).fetchall()
    deltax = db.execute(sql.format(oe="order_events")).fetchall()
    assert plain == deltax, f"plain={plain!r}  deltax={deltax!r}"
