"""Deterministic datasets used by the correctness harness."""

from __future__ import annotations

import csv
import datetime as dt
import io
import json


MOCK_NOW = "2025-01-15 12:00:00+00"
BASE_TS = "2025-01-15 00:00:00+00"
RTABENCH_BASE_TS = dt.datetime(2024, 5, 1, 0, 0, 0, tzinfo=dt.timezone.utc)
RTABENCH_MOCK_NOW = "2024-05-01 00:00:00+00"

PARTITION_SEGMENT_EDGE_ROWS = (
    ("2025-01-13 23:58:00+00", 100, 0, "default_old", -100, 1.00, "before-start-a"),
    ("2025-01-13 23:59:59.999999+00", 101, 1, "default_old", -99, None, "before-start-b"),
    ("2025-01-13 12:00:00+00", 102, None, "default_old", -98, 1.25, "before-start-c"),
    ("2025-01-14 00:00:00+00", 0, 0, "compressed_4", 0, 0.00, "p14-start"),
    ("2025-01-14 00:00:00.000001+00", 1, 1, "compressed_4", 1, 0.10, "p14-after-start"),
    ("2025-01-14 12:00:00+00", 2, None, "compressed_4", None, 0.20, "p14-mid"),
    ("2025-01-14 23:59:59.999999+00", 3, 1, "compressed_4", 3, None, "p14-end-minus"),
    ("2025-01-15 00:00:00+00", 10, 0, "compressed_5", 10, 1.00, "p15-start"),
    ("2025-01-15 00:00:00.000001+00", 11, 1, "compressed_5", 11, 1.10, "p15-after-start"),
    ("2025-01-15 06:00:00+00", 12, 2, "compressed_5", 12, None, "p15-morning"),
    ("2025-01-15 18:00:00+00", 13, None, "compressed_5", None, 1.30, "p15-evening"),
    ("2025-01-15 23:59:59.999999+00", 14, 2, "compressed_5", 14, 1.40, "p15-end-minus"),
    ("2025-01-16 00:00:00+00", 20, 0, "uncompressed_6", 20, 2.00, "p16-start"),
    ("2025-01-16 00:00:00.000001+00", 21, 1, "uncompressed_6", 21, None, "p16-after-start"),
    ("2025-01-16 04:00:00+00", 22, 2, "uncompressed_6", 22, 2.20, "p16-early"),
    ("2025-01-16 08:00:00+00", 23, None, "uncompressed_6", None, 2.30, "p16-mid"),
    ("2025-01-16 12:00:00+00", 24, 1, "uncompressed_6", 24, 2.40, "p16-noon"),
    ("2025-01-16 23:59:59.999999+00", 25, 2, "uncompressed_6", 25, 2.50, "p16-end-minus"),
    ("2025-01-17 00:00:00+00", 30, 0, "compressed_10", 30, 3.00, "p17-00"),
    ("2025-01-17 01:00:00+00", 31, 1, "compressed_10", 31, 3.10, "p17-01"),
    ("2025-01-17 02:00:00+00", 32, 2, "compressed_10", 32, None, "p17-02"),
    ("2025-01-17 03:00:00+00", 33, None, "compressed_10", None, 3.30, "p17-03"),
    ("2025-01-17 04:00:00+00", 34, 1, "compressed_10", 34, 3.40, "p17-04"),
    ("2025-01-17 05:00:00+00", 35, 2, "compressed_10", 35, 3.50, "p17-05"),
    ("2025-01-17 06:00:00+00", 36, 0, "compressed_10", 36, None, "p17-06"),
    ("2025-01-17 07:00:00+00", 37, 1, "compressed_10", 37, 3.70, "p17-07"),
    ("2025-01-17 08:00:00+00", 38, None, "compressed_10", 38, 3.80, "p17-08"),
    ("2025-01-17 23:59:59.999999+00", 39, 2, "compressed_10", 39, 3.90, "p17-end-minus"),
    ("2025-01-19 00:00:00+00", 110, 0, "default_future", 110, 4.00, "after-end-a"),
    ("2025-01-19 00:00:00.000001+00", 111, 1, "default_future", None, None, "after-end-b"),
)

PARTITION_SEGMENT_EDGE_REGISTERED_ROWS = tuple(
    row
    for row in PARTITION_SEGMENT_EDGE_ROWS
    if "default_" not in row[3]
)

CODEC_MATRIX_ROWS = tuple(
    (
        f"2025-01-15 {i // 60:02d}:{i % 60:02d}:00+00",
        i,
        None if i % 19 == 0 else i % 7,
        None
        if i % 23 == 0
        else ("alpha" if i % 4 == 0 else "beta" if i % 4 == 1 else "gamma" if i % 4 == 2 else "delta"),
        None if i % 29 == 0 else f"token-{i:04d}-{(i * 7919) % 104729:05d}",
        None if i % 17 == 0 else i % 2 == 0,
        None if i % 31 == 0 else (i % 61) - 30,
        None if i % 43 == 0 else (i % 97) - 48,
        None if i % 37 == 0 else ((i * 1_000_003) % 2_000_000_033) - 1_000_000_016,
        42 if i % 11 else -42,
        None if i % 13 == 0 else ((i % 43) - 21) / 8.0,
        f"2025-01-14 {(i // 24) % 24:02d}:{(i * 7) % 60:02d}:00+00",
        f"always-{i % 5}",
        None if i % 9 == 0 else ("" if i % 9 == 1 else f"pattern-{i % 4}"),
        None if i % 41 == 0 else (f"nullable-repeat-{i % 3}" if i % 5 else "same-value"),
    )
    for i in range(288)
) + (
    (
        "2025-01-15 10:00:00+00",
        1000,
        0,
        "alpha",
        "",
        True,
        -32768,
        -2147483648,
        -9223372036854775807,
        7,
        -0.0,
        "2025-01-14 23:59:59.999999+00",
        "",
        "",
        "",
    ),
    (
        "2025-01-15 10:00:01+00",
        1001,
        1,
        "beta",
        "text with, comma and \"quote\"",
        False,
        32767,
        2147483647,
        9223372036854775806,
        7,
        1.0e-12,
        "2025-01-14 00:00:00.000001+00",
        "always-special",
        "under_score_%_literal",
        "nullable-repeat-special",
    ),
    (
        "2025-01-15 10:00:02+00",
        1002,
        2,
        "gamma",
        "long-" + ("x" * 600),
        None,
        0,
        0,
        0,
        -7,
        1.0e12,
        "2025-01-14 12:34:56.123456+00",
        "always-long",
        None,
        "same-value",
    ),
    (
        "2025-01-15 10:00:03+00",
        1003,
        None,
        None,
        "token-boundary-null-segment",
        True,
        None,
        None,
        None,
        -7,
        -1.0e12,
        "2025-01-14 12:34:56.654321+00",
        "always-null-edge",
        None,
        None,
    ),
    (
        "2025-01-15 10:00:04+00",
        1004,
        3,
        "delta",
        "escaped-percent-100%_done",
        True,
        -1,
        -1,
        -1,
        7,
        3.1415926535,
        "2025-01-14 18:00:00+00",
        "always-symbols",
        "literal%underscore_",
        "contains,comma",
    ),
)

CODEC_MATRIX_COLUMNS = (
    "ts",
    "id",
    "device_id",
    "dict_text",
    "unique_text",
    "active",
    "small_int",
    "int_val",
    "large_int",
    "repeated_int",
    "float_val",
    "observed_at",
    "always_text",
    "pattern_text",
    "nullable_text",
)

RTABENCH_EVENT_COLUMNS = (
    "order_id",
    "counter",
    "event_created",
    "event_type",
    "satisfaction",
    "processor",
    "backup_processor",
    "event_payload",
)


def _compress_non_default_partitions(conn, table_name: str) -> None:
    partitions = conn.execute(
        f"SELECT partition_name FROM deltax.deltax_partition_info('{table_name}') "
        "WHERE partition_name NOT LIKE '%default%' ORDER BY range_start"
    ).fetchall()
    for (partition_name,) in partitions:
        row_count = conn.execute(f'SELECT count(*) FROM "{partition_name}"').fetchone()[0]
        if row_count > 0:
            conn.execute("SELECT deltax.deltax_compress_partition(%s)", (partition_name,))
    conn.commit()


def _compress_partitions_by_start_date(
    conn,
    table_name: str,
    start_dates: tuple[str, ...],
) -> None:
    for start_date in start_dates:
        rows = conn.execute(
            f"""
            SELECT partition_name
            FROM deltax.deltax_partition_info('{table_name}')
            WHERE range_start::date = %s::date
            ORDER BY range_start
            """,
            (start_date,),
        ).fetchall()
        for (partition_name,) in rows:
            row_count = conn.execute(f'SELECT count(*) FROM "{partition_name}"').fetchone()[0]
            if row_count > 0:
                conn.execute("SELECT deltax.deltax_compress_partition(%s)", (partition_name,))
    conn.commit()


def _analyze_tables(conn, *table_names: str) -> None:
    conn.rollback()
    conn.autocommit = True
    for table_name in table_names:
        conn.execute(f"ANALYZE {table_name}")
    conn.autocommit = False


def _create_partition_segment_edges_schema(conn, table_name: str) -> None:
    conn.execute(
        f"""
        CREATE TABLE {table_name} (
            ts timestamptz NOT NULL,
            id integer NOT NULL,
            device_id integer,
            bucket text,
            val integer,
            metric double precision,
            payload text
        )
        """
    )


def _insert_partition_segment_edge_rows(conn, table_name: str, rows: tuple[tuple, ...]) -> None:
    with conn.cursor() as cur:
        cur.executemany(
            f"""
            INSERT INTO {table_name} (ts, id, device_id, bucket, val, metric, payload)
            VALUES (%s::timestamptz, %s, %s, %s, %s, %s, %s)
            """,
            rows,
        )


def _copy_partition_segment_edge_rows_deltax(
    conn,
    table_name: str,
    rows: tuple[tuple, ...],
) -> None:
    buf = io.StringIO()
    writer = csv.writer(buf)
    for row in rows:
        writer.writerow("" if value is None else value for value in row)

    with conn.cursor() as cur:
        with cur.copy(
            f"COPY {table_name} FROM STDIN WITH (FORMAT deltax_compress_csv)"
        ) as copy:
            copy.write(buf.getvalue().encode())


def _create_codec_matrix_schema(conn, table_name: str) -> None:
    conn.execute(
        f"""
        CREATE TABLE {table_name} (
            ts timestamptz NOT NULL,
            id integer NOT NULL,
            device_id integer,
            dict_text text,
            unique_text text,
            active boolean,
            small_int smallint,
            int_val integer,
            large_int bigint,
            repeated_int integer NOT NULL,
            float_val double precision,
            observed_at timestamptz NOT NULL,
            always_text text NOT NULL,
            pattern_text text,
            nullable_text text
        )
        """
    )


def _insert_codec_matrix_rows(conn, table_name: str) -> None:
    with conn.cursor() as cur:
        cur.executemany(
            f"""
            INSERT INTO {table_name} (
                {", ".join(CODEC_MATRIX_COLUMNS)}
            )
            VALUES (
                %s::timestamptz, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s::timestamptz, %s, %s, %s
            )
            """,
            CODEC_MATRIX_ROWS,
        )


def _copy_codec_matrix_rows_deltax(
    conn,
    table_name: str,
    *,
    copy_format: str,
    copy_variant: str = "default",
) -> None:
    buf = io.StringIO()
    if copy_format == "deltax_compress_csv":
        null_string = "NULL" if copy_variant == "options" else r"\N"
        writer = csv.writer(buf)
        if copy_variant == "options":
            writer.writerow(CODEC_MATRIX_COLUMNS)
        for row in CODEC_MATRIX_ROWS:
            writer.writerow(null_string if value is None else value for value in row)
        copy_options = "FORMAT deltax_compress_csv"
        if copy_variant == "options":
            copy_options += ", HEADER true, NULL 'NULL'"
        else:
            copy_options += r", NULL '\N'"
        copy_sql = f"COPY {table_name} FROM STDIN WITH ({copy_options})"
    elif copy_format == "deltax_compress":
        delimiter = "|" if copy_variant == "options" else "\t"
        null_string = "NULL" if copy_variant == "options" else r"\N"
        for row in CODEC_MATRIX_ROWS:
            fields = [null_string if value is None else str(value) for value in row]
            buf.write(delimiter.join(fields))
            buf.write("\n")
        copy_options = "FORMAT deltax_compress"
        if copy_variant == "options":
            copy_options += ", DELIMITER '|', NULL 'NULL'"
        copy_sql = f"COPY {table_name} FROM STDIN WITH ({copy_options})"
    else:
        raise ValueError(f"unsupported direct backfill format: {copy_format}")

    with conn.cursor() as cur:
        with cur.copy(copy_sql) as copy:
            copy.write(buf.getvalue().encode())


def _rtabench_rows() -> tuple[tuple[tuple, ...], ...]:
    countries = ("US", "DE", "FR", "UK", "IT", "CA")
    states = ("CA", "BE", "IDF", "LND", "RM", "ON")
    categories = ("electronics", "books", "clothing", "toys", "home")
    event_types = ("Created", "Packed", "Departed", "Delivered", "Returned", "Cancelled")
    processors = ("proc-a", "proc-b", "proc-c", "proc-d")
    terminals = ("Berlin", "Hamburg", "Munich", "Frankfurt", "Cologne")
    statuses = ("Created", "Delayed", "Priority", "Delivered", "Returned", "Cancelled")

    customers = tuple(
        (
            customer_id,
            f"cust-{customer_id:03d}",
            countries[customer_id % len(countries)],
            states[customer_id % len(states)],
        )
        for customer_id in range(1, 25)
    )
    products = tuple(
        (
            product_id,
            f"prod-{product_id:03d}",
            categories[product_id % len(categories)],
            round(7.50 + ((product_id * 37) % 250) + (product_id % 4) * 0.25, 2),
            0 if product_id % 13 == 0 else 20 + ((product_id * 17) % 480),
        )
        for product_id in range(1, 41)
    )

    orders = []
    order_items = []
    events = []
    counter = 0
    for order_id in range(1, 181):
        customer_id = ((order_id * 7) % len(customers)) + 1
        created_at = RTABENCH_BASE_TS + dt.timedelta(
            days=order_id % 12,
            hours=(order_id * 5) % 24,
            minutes=(order_id * 11) % 60,
        )
        orders.append((order_id, customer_id, created_at))

        for item_idx in range(1, (order_id % 4) + 2):
            product_id = ((order_id * 3 + item_idx * 5) % len(products)) + 1
            amount = ((order_id + item_idx * 2) % 7) + 1
            order_items.append((order_id, product_id, amount))

        event_count = (order_id % 6) + 2
        for event_idx in range(event_count):
            counter += 1
            event_created = created_at + dt.timedelta(
                hours=event_idx * 6 + (order_id % 5),
                minutes=(order_id * 13 + event_idx * 17) % 60,
            )
            max_ts = RTABENCH_BASE_TS + dt.timedelta(days=15) - dt.timedelta(seconds=1)
            if event_created > max_ts:
                event_created = max_ts - dt.timedelta(minutes=event_idx)

            event_type = event_types[(order_id + event_idx) % len(event_types)]
            payload = {
                "terminal": terminals[(order_id + event_idx) % len(terminals)],
                "status": [
                    statuses[(order_id + event_idx) % len(statuses)],
                    "Priority" if order_id % 9 == 0 else "Normal",
                ],
                "lane": (order_id + event_idx) % 8 + 1,
            }
            events.append(
                (
                    order_id,
                    counter if counter % 17 else None,
                    event_created,
                    event_type,
                    round(1.0 + ((order_id * 11 + event_idx * 7) % 400) / 100.0, 2),
                    processors[(order_id + event_idx) % len(processors)],
                    None if (order_id + event_idx) % 5 == 0 else processors[
                        (order_id + event_idx + 1) % len(processors)
                    ],
                    json.dumps(payload, sort_keys=True),
                )
            )

    return (
        customers,
        products,
        tuple(orders),
        tuple(order_items),
        tuple(events),
    )


def _create_rtabench_synthetic_schema(conn, deltax_table: str, plain_table: str) -> None:
    conn.execute(
        """
        CREATE TABLE customers (
            customer_id integer PRIMARY KEY,
            name text NOT NULL,
            country text NOT NULL,
            state text NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE products (
            product_id integer PRIMARY KEY,
            name text NOT NULL,
            category text NOT NULL,
            price numeric(10,2) NOT NULL,
            stock integer NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE orders (
            order_id integer PRIMARY KEY,
            customer_id integer NOT NULL,
            created_at timestamptz NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE order_items (
            order_id integer NOT NULL,
            product_id integer NOT NULL,
            amount integer NOT NULL,
            PRIMARY KEY (order_id, product_id)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE processor_dim (
            processor text,
            region text NOT NULL,
            active boolean NOT NULL
        )
        """
    )
    for table_name in (plain_table, deltax_table):
        conn.execute(
            f"""
            CREATE TABLE {table_name} (
                order_id integer NOT NULL,
                counter integer,
                event_created timestamptz NOT NULL,
                event_type text NOT NULL,
                satisfaction real NOT NULL,
                processor text NOT NULL,
                backup_processor text,
                event_payload jsonb
            )
            """
        )


def _copy_rows_text(conn, table_name: str, rows: tuple[tuple, ...]) -> None:
    buf = io.StringIO()
    for row in rows:
        fields = []
        for value in row:
            if value is None:
                fields.append(r"\N")
            elif isinstance(value, dt.datetime):
                fields.append(value.isoformat())
            else:
                fields.append(str(value).replace("\\", "\\\\").replace("\t", r"\t"))
        buf.write("\t".join(fields))
        buf.write("\n")

    with conn.cursor() as cur:
        with cur.copy(f"COPY {table_name} FROM STDIN") as copy:
            copy.write(buf.getvalue().encode())


def _insert_rtabench_events(conn, table_name: str, events: tuple[tuple, ...]) -> None:
    for event in events:
        conn.execute(
            f"""
            INSERT INTO {table_name} (
                order_id,
                counter,
                event_created,
                event_type,
                satisfaction,
                processor,
                backup_processor,
                event_payload
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb)
            """,
            event,
        )


def _copy_rtabench_events_deltax(
    conn,
    table_name: str,
    events: tuple[tuple, ...],
    *,
    copy_format: str,
) -> None:
    buf = io.StringIO()
    if copy_format == "deltax_compress_csv":
        writer = csv.writer(buf)
        for row in events:
            writer.writerow(r"\N" if value is None else value for value in row)
        copy_sql = f"COPY {table_name} FROM STDIN WITH (FORMAT deltax_compress_csv, NULL '\\N')"
    elif copy_format == "deltax_compress":
        for row in events:
            fields = []
            for value in row:
                if value is None:
                    fields.append(r"\N")
                elif isinstance(value, dt.datetime):
                    fields.append(value.isoformat())
                else:
                    fields.append(str(value).replace("\\", "\\\\").replace("\t", r"\t"))
            buf.write("\t".join(fields))
            buf.write("\n")
        copy_sql = f"COPY {table_name} FROM STDIN WITH (FORMAT deltax_compress)"
    else:
        raise ValueError(f"unsupported rtabench load path: {copy_format}")

    with conn.cursor() as cur:
        with cur.copy(copy_sql) as copy:
            copy.write(buf.getvalue().encode())


def create_tiny_events_pair(conn, *, segment_size: int = 16) -> tuple[str, str]:
    """Create a small postgres/deltax table pair and compress the deltax side."""
    conn.execute(f"SET pg_deltax.mock_now = '{MOCK_NOW}'")
    conn.execute(
        """
        CREATE TABLE events_plain (
            ts timestamptz NOT NULL,
            id integer NOT NULL,
            device_id integer,
            kind text,
            val integer,
            metric double precision
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE events (
            ts timestamptz NOT NULL,
            id integer NOT NULL,
            device_id integer,
            kind text,
            val integer,
            metric double precision
        )
        """
    )
    conn.execute("SELECT deltax.deltax_create_table('events', 'ts', '1 day'::interval, 3)")
    conn.execute(
        "SELECT deltax.deltax_enable_compression("
        "'events', segment_by => ARRAY['device_id'], "
        "order_by => ARRAY['ts', 'id'], segment_size => %s)",
        (segment_size,),
    )
    conn.commit()

    insert_sql = f"""
        INSERT INTO {{table}} (ts, id, device_id, kind, val, metric)
        SELECT
            '{BASE_TS}'::timestamptz + (i * interval '1 minute') AS ts,
            i AS id,
            CASE WHEN i % 11 = 0 THEN NULL ELSE i % 5 END AS device_id,
            CASE
                WHEN i % 13 = 0 THEN NULL
                WHEN i % 3 = 0 THEN 'alpha'
                WHEN i % 3 = 1 THEN 'beta'
                ELSE 'gamma'
            END AS kind,
            CASE WHEN i % 17 = 0 THEN NULL ELSE (i % 23) - 11 END AS val,
            CASE WHEN i % 19 = 0 THEN NULL ELSE (i::float8 / 10.0) END AS metric
        FROM generate_series(0, 95) AS g(i)
    """
    conn.execute(insert_sql.format(table="events_plain"))
    conn.execute(insert_sql.format(table="events"))
    conn.commit()

    _compress_non_default_partitions(conn, "events")
    _analyze_tables(conn, "events_plain", "events")

    return "events_plain", "events"


def create_predicate_matrix_pair(
    conn,
    *,
    deltax_table: str = "predicate_events",
    order_by: tuple[str, ...] = ("ts", "id"),
    segment_size: int = 8,
) -> tuple[str, str]:
    """Create a deterministic scalar predicate dataset and compress it."""
    plain_table = f"{deltax_table}_plain"
    order_by_sql = ", ".join(f"'{column}'" for column in order_by)

    conn.execute(f"SET pg_deltax.mock_now = '{MOCK_NOW}'")
    for table_name in (plain_table, deltax_table):
        conn.execute(
            f"""
            CREATE TABLE {table_name} (
                ts timestamptz NOT NULL,
                id integer NOT NULL,
                device_id integer,
                int_val integer,
                low_text text,
                high_text text,
                active boolean,
                score double precision,
                code text
            )
            """
        )

    conn.execute(f"SELECT deltax.deltax_create_table('{deltax_table}', 'ts', '1 day'::interval, 3)")
    conn.execute(
        "SELECT deltax.deltax_enable_compression("
        f"'{deltax_table}', segment_by => ARRAY['device_id'], "
        f"order_by => ARRAY[{order_by_sql}], segment_size => %s)",
        (segment_size,),
    )
    conn.commit()

    insert_sql = f"""
        INSERT INTO {{table}} (
            ts, id, device_id, int_val, low_text, high_text, active, score, code
        )
        SELECT
            '{BASE_TS}'::timestamptz + (i * interval '5 minutes') AS ts,
            i AS id,
            CASE WHEN i % 10 = 0 THEN NULL ELSE i % 6 END AS device_id,
            CASE WHEN i % 12 = 0 THEN NULL ELSE (i % 41) - 20 END AS int_val,
            CASE
                WHEN i % 14 = 0 THEN NULL
                WHEN i % 4 = 0 THEN 'red'
                WHEN i % 4 = 1 THEN 'blue'
                WHEN i % 4 = 2 THEN 'green'
                ELSE 'amber'
            END AS low_text,
            CASE
                WHEN i % 15 = 0 THEN NULL
                WHEN i % 5 = 0 THEN 'prefix-' || lpad(i::text, 3, '0') || '-tail'
                WHEN i % 5 = 1 THEN 'middle-' || lpad(i::text, 3, '0') || '-contains'
                ELSE 'token-' || lpad(i::text, 3, '0')
            END AS high_text,
            CASE WHEN i % 9 = 0 THEN NULL ELSE i % 2 = 0 END AS active,
            CASE WHEN i % 16 = 0 THEN NULL ELSE ((i % 37) - 18)::float8 / 3.0 END AS score,
            CASE WHEN i % 13 = 0 THEN NULL ELSE ((i % 50) + 100)::text END AS code
        FROM generate_series(0, 143) AS g(i)
    """
    conn.execute(insert_sql.format(table=plain_table))
    conn.execute(insert_sql.format(table=deltax_table))
    conn.commit()

    _compress_non_default_partitions(conn, deltax_table)
    _analyze_tables(conn, plain_table, deltax_table)

    return plain_table, deltax_table


def create_ordering_edges_pair(
    conn,
    *,
    deltax_table: str = "ordering_edges",
    order_by: tuple[str, ...] = ("ts",),
    segment_size: int = 12,
) -> tuple[str, str]:
    """Create rows with repeated/NULL sort keys for Top-N correctness."""
    plain_table = f"{deltax_table}_plain"
    order_by_sql = ", ".join(f"'{column}'" for column in order_by)

    conn.execute(f"SET pg_deltax.mock_now = '{MOCK_NOW}'")
    for table_name in (plain_table, deltax_table):
        conn.execute(
            f"""
            CREATE TABLE {table_name} (
                ts timestamptz NOT NULL,
                id integer NOT NULL,
                device_id integer,
                sort_val integer,
                text_sort text,
                tie_val integer NOT NULL,
                payload text,
                extra text,
                active boolean,
                metric double precision
            )
            """
        )

    conn.execute(f"SELECT deltax.deltax_create_table('{deltax_table}', 'ts', '1 day'::interval, 3)")
    conn.execute(
        "SELECT deltax.deltax_enable_compression("
        f"'{deltax_table}', segment_by => ARRAY['device_id'], "
        f"order_by => ARRAY[{order_by_sql}], segment_size => %s)",
        (segment_size,),
    )
    conn.commit()

    insert_sql = f"""
        INSERT INTO {{table}} (
            ts, id, device_id, sort_val, text_sort, tie_val, payload, extra, active, metric
        )
        SELECT
            '{BASE_TS}'::timestamptz + ((i % 48) * interval '2 minutes') AS ts,
            i AS id,
            CASE WHEN i % 17 = 0 THEN NULL ELSE i % 7 END AS device_id,
            CASE WHEN i % 11 = 0 THEN NULL ELSE (i % 19) - 9 END AS sort_val,
            CASE
                WHEN i % 10 = 0 THEN NULL
                WHEN i % 5 = 0 THEN 'echo-' || lpad((i % 23)::text, 2, '0')
                WHEN i % 5 = 1 THEN 'bravo-' || lpad((i % 17)::text, 2, '0')
                WHEN i % 5 = 2 THEN 'delta-' || lpad((i % 19)::text, 2, '0')
                WHEN i % 5 = 3 THEN 'alpha-' || lpad((i % 13)::text, 2, '0')
                ELSE 'charlie-' || lpad((i % 11)::text, 2, '0')
            END AS text_sort,
            i % 5 AS tie_val,
            CASE
                WHEN i % 4 = 0 THEN 'alpha-' || lpad(i::text, 3, '0')
                WHEN i % 4 = 1 THEN 'beta-' || lpad(i::text, 3, '0')
                WHEN i % 4 = 2 THEN 'gamma-' || lpad(i::text, 3, '0')
                ELSE 'delta-' || lpad(i::text, 3, '0')
            END AS payload,
            repeat(chr(65 + (i % 26)), 3) || '-' || (191 - i)::text AS extra,
            i % 3 <> 0 AS active,
            CASE WHEN i % 13 = 0 THEN NULL ELSE ((i % 31) - 15)::float8 / 4.0 END AS metric
        FROM generate_series(0, 191) AS g(i)
    """
    conn.execute(insert_sql.format(table=plain_table))
    conn.execute(insert_sql.format(table=deltax_table))
    conn.commit()

    _compress_non_default_partitions(conn, deltax_table)
    _analyze_tables(conn, plain_table, deltax_table)

    return plain_table, deltax_table


def create_aggregate_matrix_pair(
    conn,
    *,
    deltax_table: str = "aggregate_matrix",
    segment_by: tuple[str, ...] = ("group_key",),
    order_by: tuple[str, ...] = ("ts", "id"),
    segment_size: int = 10,
    row_count: int = 216,
) -> tuple[str, str]:
    """Create a numeric-heavy aggregate dataset and compress it."""
    plain_table = f"{deltax_table}_plain"
    segment_by_sql = ", ".join(f"'{column}'" for column in segment_by)
    order_by_sql = ", ".join(f"'{column}'" for column in order_by)
    if row_count <= 0:
        raise ValueError("row_count must be positive")

    conn.execute(f"SET pg_deltax.mock_now = '{MOCK_NOW}'")
    for table_name in (plain_table, deltax_table):
        conn.execute(
            f"""
            CREATE TABLE {table_name} (
                ts timestamptz NOT NULL,
                id integer NOT NULL,
                group_key integer,
                sub_key integer,
                device_id integer,
                bucket_not_null integer NOT NULL,
                int_not_null integer NOT NULL,
                int_nullable integer,
                all_null_input integer,
                repeat_val integer,
                float_val double precision,
                filter_val integer
            )
            """
        )

    conn.execute(f"SELECT deltax.deltax_create_table('{deltax_table}', 'ts', '1 day'::interval, 3)")
    conn.execute(
        "SELECT deltax.deltax_enable_compression("
        f"'{deltax_table}', segment_by => ARRAY[{segment_by_sql}], "
        f"order_by => ARRAY[{order_by_sql}], segment_size => %s)",
        (segment_size,),
    )
    conn.commit()

    insert_sql = f"""
        INSERT INTO {{table}} (
            ts, id, group_key, sub_key, device_id, bucket_not_null,
            int_not_null, int_nullable, all_null_input, repeat_val, float_val, filter_val
        )
        SELECT
            '{BASE_TS}'::timestamptz + (i * interval '20 minutes') AS ts,
            i AS id,
            CASE WHEN i % 29 = 0 THEN NULL ELSE i % 6 END AS group_key,
            CASE WHEN i % 13 = 0 THEN NULL ELSE i % 4 END AS sub_key,
            CASE WHEN i % 17 = 0 THEN NULL ELSE i % 8 END AS device_id,
            i % 6 AS bucket_not_null,
            (i % 43) - 21 AS int_not_null,
            CASE WHEN i % 7 = 0 THEN NULL ELSE (i % 37) - 18 END AS int_nullable,
            CASE WHEN i % 6 = 5 THEN NULL ELSE (i % 23) - 11 END AS all_null_input,
            (i % 5) - 2 AS repeat_val,
            CASE WHEN i % 11 = 0 THEN NULL ELSE ((i % 41) - 20)::float8 / 7.0 END AS float_val,
            (i % 19) - 9 AS filter_val
        FROM generate_series(0, {row_count - 1}) AS g(i)
    """
    conn.execute(insert_sql.format(table=plain_table))
    conn.execute(insert_sql.format(table=deltax_table))
    conn.commit()

    _compress_non_default_partitions(conn, deltax_table)
    _analyze_tables(conn, plain_table, deltax_table)

    return plain_table, deltax_table


def create_partition_segment_edges_pair(
    conn,
    *,
    deltax_table: str = "partition_segment_edges",
    segment_size: int = 5,
) -> tuple[str, str]:
    """Create a mixed compressed/uncompressed layout around partition edges."""
    plain_table = f"{deltax_table}_plain"

    conn.execute(f"SET pg_deltax.mock_now = '{MOCK_NOW}'")
    for table_name in (plain_table, deltax_table):
        _create_partition_segment_edges_schema(conn, table_name)

    conn.execute(f"SELECT deltax.deltax_create_table('{deltax_table}', 'ts', '1 day'::interval, 3)")
    conn.execute(
        "SELECT deltax.deltax_enable_compression("
        f"'{deltax_table}', segment_by => ARRAY['device_id'], "
        "order_by => ARRAY['ts', 'id'], segment_size => %s)",
        (segment_size,),
    )
    conn.commit()

    _insert_partition_segment_edge_rows(conn, plain_table, PARTITION_SEGMENT_EDGE_ROWS)
    _insert_partition_segment_edge_rows(conn, deltax_table, PARTITION_SEGMENT_EDGE_ROWS)
    conn.commit()

    _compress_partitions_by_start_date(
        conn,
        deltax_table,
        ("2025-01-14", "2025-01-15", "2025-01-17"),
    )
    _analyze_tables(conn, plain_table, deltax_table)

    return plain_table, deltax_table


def create_partition_segment_edges_direct_backfill_pair(
    conn,
    *,
    deltax_table: str = "partition_segment_edges_direct",
    segment_size: int = 5,
) -> tuple[str, str]:
    """Create registered partition edge rows via direct compressed COPY."""
    plain_table = f"{deltax_table}_plain"

    conn.execute(f"SET pg_deltax.mock_now = '{MOCK_NOW}'")
    for table_name in (plain_table, deltax_table):
        _create_partition_segment_edges_schema(conn, table_name)

    conn.execute(f"SELECT deltax.deltax_create_table('{deltax_table}', 'ts', '1 day'::interval, 3)")
    conn.execute(
        "SELECT deltax.deltax_enable_compression("
        f"'{deltax_table}', segment_by => ARRAY[]::text[], "
        "order_by => ARRAY['ts', 'id'], segment_size => %s)",
        (segment_size,),
    )
    conn.commit()

    _insert_partition_segment_edge_rows(
        conn,
        plain_table,
        PARTITION_SEGMENT_EDGE_REGISTERED_ROWS,
    )
    _copy_partition_segment_edge_rows_deltax(
        conn,
        deltax_table,
        PARTITION_SEGMENT_EDGE_REGISTERED_ROWS,
    )
    conn.commit()
    _analyze_tables(conn, plain_table, deltax_table)

    return plain_table, deltax_table


def create_codec_matrix_pair(
    conn,
    *,
    deltax_table: str = "codec_matrix",
    load_path: str = "regular",
    segment_by: tuple[str, ...] = ("device_id",),
    order_by: tuple[str, ...] = ("ts", "id"),
    segment_size: int = 9,
) -> tuple[str, str]:
    """Create codec-targeted rows using regular compression or direct backfill."""
    plain_table = f"{deltax_table}_plain"
    segment_by_sql = (
        "ARRAY[]::text[]"
        if not segment_by
        else "ARRAY[" + ", ".join(f"'{column}'" for column in segment_by) + "]"
    )
    order_by_sql = ", ".join(f"'{column}'" for column in order_by)

    conn.execute(f"SET pg_deltax.mock_now = '{MOCK_NOW}'")
    for table_name in (plain_table, deltax_table):
        _create_codec_matrix_schema(conn, table_name)

    conn.execute(f"SELECT deltax.deltax_create_table('{deltax_table}', 'ts', '1 day'::interval, 3)")
    conn.execute(
        "SELECT deltax.deltax_enable_compression("
        f"'{deltax_table}', segment_by => {segment_by_sql}, "
        f"order_by => ARRAY[{order_by_sql}], segment_size => %s)",
        (segment_size,),
    )
    conn.commit()

    _insert_codec_matrix_rows(conn, plain_table)

    if load_path == "regular":
        _insert_codec_matrix_rows(conn, deltax_table)
        conn.commit()
        _compress_non_default_partitions(conn, deltax_table)
    elif load_path == "copy_text":
        _copy_codec_matrix_rows_deltax(
            conn,
            deltax_table,
            copy_format="deltax_compress",
        )
        conn.commit()
    elif load_path == "copy_csv":
        _copy_codec_matrix_rows_deltax(
            conn,
            deltax_table,
            copy_format="deltax_compress_csv",
        )
        conn.commit()
    elif load_path == "copy_text_options":
        _copy_codec_matrix_rows_deltax(
            conn,
            deltax_table,
            copy_format="deltax_compress",
            copy_variant="options",
        )
        conn.commit()
    elif load_path == "copy_csv_options":
        _copy_codec_matrix_rows_deltax(
            conn,
            deltax_table,
            copy_format="deltax_compress_csv",
            copy_variant="options",
        )
        conn.commit()
    else:
        raise ValueError(f"unsupported codec matrix load path: {load_path}")

    _analyze_tables(conn, plain_table, deltax_table)

    return plain_table, deltax_table


def create_rtabench_synthetic_pair(
    conn,
    *,
    deltax_table: str = "order_events",
    load_path: str = "copy_text",
    segment_size: int = 25,
    mixed_uncompressed_tail: bool = False,
) -> tuple[str, str]:
    """Create an RTABench-shaped dimensional dataset and compressed fact table."""
    plain_table = f"{deltax_table}_plain"

    conn.execute(f"SET pg_deltax.mock_now = '{RTABENCH_MOCK_NOW}'")
    _create_rtabench_synthetic_schema(conn, deltax_table, plain_table)
    conn.execute(
        f"SELECT deltax.deltax_create_table('{deltax_table}', 'event_created', '1 day'::interval, 16)"
    )
    conn.execute(
        "SELECT deltax.deltax_enable_compression("
        f"'{deltax_table}', segment_by => ARRAY[]::text[], "
        "order_by => ARRAY['order_id', 'event_created'], segment_size => %s)",
        (segment_size,),
    )
    conn.commit()

    customers, products, orders, order_items, events = _rtabench_rows()
    _copy_rows_text(conn, "customers", customers)
    _copy_rows_text(conn, "products", products)
    _copy_rows_text(conn, "orders", orders)
    _copy_rows_text(conn, "order_items", order_items)
    _copy_rows_text(
        conn,
        "processor_dim",
        (
            ("proc-a", "eu-central", True),
            ("proc-b", "us-east", True),
            ("proc-c", "ap-south", False),
            ("proc-d", "eu-west", True),
            (None, "missing-backup", False),
        ),
    )
    _copy_rows_text(conn, plain_table, events)

    direct_events = events
    tail_events: tuple[tuple, ...] = ()
    if mixed_uncompressed_tail:
        tail_date = max(event[2].date() for event in events)
        direct_events = tuple(event for event in events if event[2].date() != tail_date)
        tail_events = tuple(event for event in events if event[2].date() == tail_date)

    if load_path == "copy_text":
        _copy_rtabench_events_deltax(
            conn,
            deltax_table,
            direct_events,
            copy_format="deltax_compress",
        )
    elif load_path == "copy_csv":
        _copy_rtabench_events_deltax(
            conn,
            deltax_table,
            direct_events,
            copy_format="deltax_compress_csv",
        )
    elif load_path == "regular":
        _copy_rows_text(conn, deltax_table, events)
        conn.commit()
        _compress_non_default_partitions(conn, deltax_table)
    else:
        raise ValueError(f"unsupported rtabench load path: {load_path}")

    if tail_events:
        _insert_rtabench_events(conn, deltax_table, tail_events)
    conn.commit()

    default_rows = conn.execute(f"SELECT count(*) FROM {deltax_table}_default").fetchone()[0]
    if default_rows:
        raise AssertionError(f"{default_rows} RTABench rows landed in the default partition")

    _analyze_tables(
        conn,
        "customers",
        "products",
        "processor_dim",
        "orders",
        "order_items",
        plain_table,
        deltax_table,
    )

    return plain_table, deltax_table
