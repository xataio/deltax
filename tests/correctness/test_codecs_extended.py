"""Extended codec and type correctness coverage."""

import csv
import io

import pytest

from .harness import QueryCase, assert_query_case


pytestmark = pytest.mark.extended


def _copy_csv(conn, table_name, rows, *, copy_format="csv"):
    buf = io.StringIO()
    writer = csv.writer(buf)
    for row in rows:
        writer.writerow(r"\N" if value is None else value for value in row)
    with conn.cursor() as cur:
        with cur.copy(f"COPY {table_name} FROM STDIN WITH (FORMAT {copy_format}, NULL '\\N')") as copy:
            copy.write(buf.getvalue().encode())


def _compress_all_non_empty_partitions(conn, table_name):
    partitions = conn.execute(
        f"""
        SELECT partition_name
        FROM deltax.deltax_partition_info('{table_name}')
        ORDER BY partition_name
        """
    ).fetchall()
    for (partition_name,) in partitions:
        row_count = conn.execute(f'SELECT count(*) FROM "{partition_name}"').fetchone()[0]
        if row_count > 0:
            conn.execute("SELECT deltax.deltax_compress_partition(%s)", (partition_name,))


@pytest.fixture()
def fallback_type_matrix(db):
    db.execute("SET pg_deltax.mock_now = '2025-01-20 12:00:00+00'")
    for table_name in ("fallback_type_matrix_plain", "fallback_type_matrix"):
        db.execute(
            f"""
            CREATE TABLE {table_name} (
                ts timestamptz NOT NULL,
                id integer NOT NULL,
                date_val date,
                float_val double precision
            )
            """
        )
    db.execute("SELECT deltax.deltax_create_table('fallback_type_matrix', 'ts', '1 day'::interval, 3)")
    db.execute(
        "SELECT deltax.deltax_enable_compression("
        "'fallback_type_matrix', segment_by => ARRAY[]::text[], "
        "order_by => ARRAY['ts', 'id'], segment_size => 4)"
    )
    db.commit()

    insert_sql = """
        INSERT INTO {table} (
            ts, id, date_val, float_val
        )
        VALUES
            ('2025-01-20 00:00:00+00', 1, '2025-01-20', 10.25::float8),
            ('2025-01-20 01:00:00+00', 2, '2025-01-21', -20.5::float8),
            ('2025-01-21 00:00:00+00', 3, NULL, NULL),
            ('2025-01-21 02:00:00+00', 4, '2025-01-22', -0.0::float8),
            ('2025-01-22 00:00:00+00', 5, '2025-01-23', 1.5::float8)
    """
    db.execute(insert_sql.format(table="fallback_type_matrix_plain"))
    db.execute(insert_sql.format(table="fallback_type_matrix"))
    db.commit()
    _compress_all_non_empty_partitions(db, "fallback_type_matrix")
    db.execute("ANALYZE fallback_type_matrix_plain")
    db.execute("ANALYZE fallback_type_matrix")
    return "fallback_type_matrix_plain", "fallback_type_matrix"


def test_fallback_type_matrix_matches_plain_postgres(fallback_type_matrix, db):
    plain_table, deltax_table = fallback_type_matrix
    cases = (
        QueryCase(
            "fallback_type_projection",
            """
                SELECT
                    id,
                    date_val,
                    float_val::text AS float_text
            FROM {table}
            ORDER BY ts, id
            """,
        ),
        QueryCase(
            "fallback_type_predicates",
            """
            SELECT id, date_val
            FROM {table}
            WHERE date_val IS NULL
               OR date_val >= DATE '2025-01-17'
            ORDER BY id
            """,
        ),
        QueryCase(
            "finite_float_values_as_text",
            """
            SELECT id, float_val::text
            FROM {table}
            WHERE float_val::text IN ('10.25', '-20.5', '0')
               OR float_val = 1.5
            ORDER BY id
            """,
        ),
    )
    for case in cases:
        assert_query_case(db, case, plain_table=plain_table, deltax_table=deltax_table)


def test_direct_csv_backfill_quoted_text_edges_match_plain_postgres(db):
    db.execute("SET pg_deltax.mock_now = '2025-01-20 12:00:00+00'")
    for table_name in ("csv_text_edges_plain", "csv_text_edges"):
        db.execute(
            f"""
            CREATE TABLE {table_name} (
                ts timestamptz NOT NULL,
                id integer NOT NULL,
                payload text,
                note text
            )
            """
        )
    db.execute("SELECT deltax.deltax_create_table('csv_text_edges', 'ts', '1 day'::interval, 3)")
    db.execute(
        "SELECT deltax.deltax_enable_compression("
        "'csv_text_edges', segment_by => ARRAY[]::text[], "
        "order_by => ARRAY['ts', 'id'], segment_size => 2)"
    )
    db.commit()

    rows = (
        ("2025-01-20 00:00:00+00", 1, 'comma,value', 'quote " inside'),
        ("2025-01-20 00:01:00+00", 2, "line\nbreak", "backslash \\ path"),
        ("2025-01-21 00:00:00+00", 3, "", None),
        ("2025-01-21 00:01:00+00", 4, None, "contains,comma\nand newline"),
    )
    _copy_csv(db, "csv_text_edges_plain", rows, copy_format="csv")
    _copy_csv(db, "csv_text_edges", rows, copy_format="deltax_compress_csv")
    db.commit()
    db.execute("ANALYZE csv_text_edges_plain")
    db.execute("ANALYZE csv_text_edges")

    assert_query_case(
        db,
        QueryCase(
            "direct_csv_quoted_text_edges",
            """
            SELECT id, payload, note, length(payload), length(note)
            FROM {table}
            ORDER BY ts, id
            """,
        ),
        plain_table="csv_text_edges_plain",
        deltax_table="csv_text_edges",
    )


def test_direct_backfill_malformed_input_rolls_back_cleanly(db):
    db.execute("SET pg_deltax.mock_now = '2025-01-20 12:00:00+00'")
    db.execute(
        """
        CREATE TABLE malformed_direct (
            ts timestamptz NOT NULL,
            id integer NOT NULL,
            val integer
        )
        """
    )
    db.execute("SELECT deltax.deltax_create_table('malformed_direct', 'ts', '1 day'::interval, 3)")
    db.execute(
        "SELECT deltax.deltax_enable_compression("
        "'malformed_direct', segment_by => ARRAY[]::text[], "
        "order_by => ARRAY['ts', 'id'], segment_size => 2)"
    )
    db.commit()

    with pytest.raises(Exception):
        with db.cursor() as cur:
            with cur.copy("COPY malformed_direct FROM STDIN WITH (FORMAT deltax_compress)") as copy:
                copy.write(b"2025-01-20T00:00:00+00\t1\t10\n")
                copy.write(b"2025-01-20T00:01:00+00\tbad-integer\t11\n")
    db.rollback()

    rows = db.execute("SELECT count(*) FROM malformed_direct").fetchone()[0]
    assert rows == 0


@pytest.mark.xfail(strict=True, reason="NaN float colstats are emitted as an unquoted SQL token")
def test_float_special_nan_compression_regression(db):
    db.execute("SET pg_deltax.mock_now = '2025-01-20 12:00:00+00'")
    db.execute(
        """
        CREATE TABLE float_special_nan (
            ts timestamptz NOT NULL,
            id integer NOT NULL,
            val double precision
        )
        """
    )
    db.execute("SELECT deltax.deltax_create_table('float_special_nan', 'ts', '1 day'::interval, 3)")
    db.execute(
        "SELECT deltax.deltax_enable_compression("
        "'float_special_nan', segment_by => ARRAY[]::text[], "
        "order_by => ARRAY['ts', 'id'], segment_size => 2)"
    )
    db.commit()
    db.execute(
        """
        INSERT INTO float_special_nan VALUES
            ('2025-01-20 00:00:00+00', 1, 'NaN'::float8),
            ('2025-01-20 00:01:00+00', 2, 'Infinity'::float8)
        """
    )
    db.commit()
    partition_name = db.execute(
        """
        SELECT partition_name
        FROM deltax.deltax_partition_info('float_special_nan')
        WHERE range_start = '2025-01-20 00:00:00+00'
        """
    ).fetchone()[0]
    db.execute("SELECT deltax.deltax_compress_partition(%s)", (partition_name,))


@pytest.mark.xfail(strict=True, reason="direct backfill currently mis-restores NULL segment_by values")
def test_direct_backfill_null_segment_by_regression(db):
    db.execute("SET pg_deltax.mock_now = '2025-01-20 12:00:00+00'")
    for table_name in ("direct_null_segment_plain", "direct_null_segment"):
        db.execute(
            f"""
            CREATE TABLE {table_name} (
                ts timestamptz NOT NULL,
                id integer NOT NULL,
                device_id integer,
                payload text
            )
            """
        )
    db.execute("SELECT deltax.deltax_create_table('direct_null_segment', 'ts', '1 day'::interval, 3)")
    db.execute(
        "SELECT deltax.deltax_enable_compression("
        "'direct_null_segment', segment_by => ARRAY['device_id'], "
        "order_by => ARRAY['ts', 'id'], segment_size => 3)"
    )
    db.commit()

    rows = (
        ("2025-01-20 00:00:00+00", 1, 1, "nonnull"),
        ("2025-01-20 00:10:00+00", 2, None, "null-segment"),
    )
    _copy_csv(db, "direct_null_segment_plain", rows, copy_format="csv")
    _copy_csv(db, "direct_null_segment", rows, copy_format="deltax_compress_csv")
    db.commit()
    assert_query_case(
        db,
        QueryCase(
            "direct_backfill_null_segment_by",
            """
            SELECT id, ts, device_id, payload
            FROM {table}
            ORDER BY ts, id
            """,
        ),
        plain_table="direct_null_segment_plain",
        deltax_table="direct_null_segment",
    )


@pytest.mark.xfail(strict=True, reason="numeric fallback columns currently decode with invalid numeric text")
def test_numeric_fallback_type_regression(db):
    db.execute("SET pg_deltax.mock_now = '2025-01-20 12:00:00+00'")
    for table_name in ("numeric_fallback_plain", "numeric_fallback"):
        db.execute(
            f"""
            CREATE TABLE {table_name} (
                ts timestamptz NOT NULL,
                id integer NOT NULL,
                numeric_val numeric(20, 6)
            )
            """
        )
    db.execute("SELECT deltax.deltax_create_table('numeric_fallback', 'ts', '1 day'::interval, 3)")
    db.execute(
        "SELECT deltax.deltax_enable_compression("
        "'numeric_fallback', segment_by => ARRAY[]::text[], "
        "order_by => ARRAY['ts', 'id'], segment_size => 2)"
    )
    db.commit()
    db.execute(
        """
        INSERT INTO numeric_fallback_plain VALUES
            ('2025-01-20 00:00:00+00', 1, 123.456789),
            ('2025-01-20 00:01:00+00', 2, -999999.000001)
        """
    )
    db.execute(
        """
        INSERT INTO numeric_fallback VALUES
            ('2025-01-20 00:00:00+00', 1, 123.456789),
            ('2025-01-20 00:01:00+00', 2, -999999.000001)
        """
    )
    db.commit()
    partition_name = db.execute(
        """
        SELECT partition_name
        FROM deltax.deltax_partition_info('numeric_fallback')
        WHERE range_start = '2025-01-20 00:00:00+00'
        """
    ).fetchone()[0]
    db.execute("SELECT deltax.deltax_compress_partition(%s)", (partition_name,))
    assert_query_case(
        db,
        QueryCase(
            "numeric_fallback_projection",
            """
            SELECT id, numeric_val
            FROM {table}
            ORDER BY id
            """,
        ),
        plain_table="numeric_fallback_plain",
        deltax_table="numeric_fallback",
    )


@pytest.mark.xfail(strict=True, reason="time fallback columns currently decode to invalid time values")
def test_time_fallback_type_regression(db):
    db.execute("SET pg_deltax.mock_now = '2025-01-20 12:00:00+00'")
    for table_name in ("time_fallback_plain", "time_fallback"):
        db.execute(
            f"""
            CREATE TABLE {table_name} (
                ts timestamptz NOT NULL,
                id integer NOT NULL,
                time_val time
            )
            """
        )
    db.execute("SELECT deltax.deltax_create_table('time_fallback', 'ts', '1 day'::interval, 3)")
    db.execute(
        "SELECT deltax.deltax_enable_compression("
        "'time_fallback', segment_by => ARRAY[]::text[], "
        "order_by => ARRAY['ts', 'id'], segment_size => 2)"
    )
    db.commit()
    for table_name in ("time_fallback_plain", "time_fallback"):
        db.execute(
            f"""
            INSERT INTO {table_name} VALUES
                ('2025-01-20 00:00:00+00', 1, '00:00:00'::time),
                ('2025-01-20 00:01:00+00', 2, '12:34:56.789'::time)
            """
        )
    db.commit()
    partition_name = db.execute(
        """
        SELECT partition_name
        FROM deltax.deltax_partition_info('time_fallback')
        WHERE range_start = '2025-01-20 00:00:00+00'
        """
    ).fetchone()[0]
    db.execute("SELECT deltax.deltax_compress_partition(%s)", (partition_name,))
    assert_query_case(
        db,
        QueryCase(
            "time_fallback_projection",
            """
            SELECT id, time_val
            FROM {table}
            ORDER BY id
            """,
        ),
        plain_table="time_fallback_plain",
        deltax_table="time_fallback",
    )


@pytest.mark.xfail(strict=True, reason="uuid fallback columns currently decode from raw text bytes")
def test_uuid_fallback_type_regression(db):
    db.execute("SET pg_deltax.mock_now = '2025-01-20 12:00:00+00'")
    for table_name in ("uuid_fallback_plain", "uuid_fallback"):
        db.execute(
            f"""
            CREATE TABLE {table_name} (
                ts timestamptz NOT NULL,
                id integer NOT NULL,
                uuid_val uuid
            )
            """
        )
    db.execute("SELECT deltax.deltax_create_table('uuid_fallback', 'ts', '1 day'::interval, 3)")
    db.execute(
        "SELECT deltax.deltax_enable_compression("
        "'uuid_fallback', segment_by => ARRAY[]::text[], "
        "order_by => ARRAY['ts', 'id'], segment_size => 2)"
    )
    db.commit()
    for table_name in ("uuid_fallback_plain", "uuid_fallback"):
        db.execute(
            f"""
            INSERT INTO {table_name} VALUES
                ('2025-01-20 00:00:00+00', 1, '00000000-0000-0000-0000-000000000001'),
                ('2025-01-20 00:01:00+00', 2, 'ffffffff-ffff-ffff-ffff-ffffffffffff')
            """
        )
    db.commit()
    partition_name = db.execute(
        """
        SELECT partition_name
        FROM deltax.deltax_partition_info('uuid_fallback')
        WHERE range_start = '2025-01-20 00:00:00+00'
        """
    ).fetchone()[0]
    db.execute("SELECT deltax.deltax_compress_partition(%s)", (partition_name,))
    assert_query_case(
        db,
        QueryCase(
            "uuid_fallback_projection",
            """
            SELECT id, uuid_val
            FROM {table}
            ORDER BY id
            """,
        ),
        plain_table="uuid_fallback_plain",
        deltax_table="uuid_fallback",
    )


@pytest.mark.xfail(strict=True, reason="bytea fallback columns currently decode escaped text bytes")
def test_bytea_fallback_type_regression(db):
    db.execute("SET pg_deltax.mock_now = '2025-01-20 12:00:00+00'")
    for table_name in ("bytea_fallback_plain", "bytea_fallback"):
        db.execute(
            f"""
            CREATE TABLE {table_name} (
                ts timestamptz NOT NULL,
                id integer NOT NULL,
                bytea_val bytea
            )
            """
        )
    db.execute("SELECT deltax.deltax_create_table('bytea_fallback', 'ts', '1 day'::interval, 3)")
    db.execute(
        "SELECT deltax.deltax_enable_compression("
        "'bytea_fallback', segment_by => ARRAY[]::text[], "
        "order_by => ARRAY['ts', 'id'], segment_size => 2)"
    )
    db.commit()
    for table_name in ("bytea_fallback_plain", "bytea_fallback"):
        db.execute(
            f"""
            INSERT INTO {table_name} VALUES
                ('2025-01-20 00:00:00+00', 1, decode('00ff', 'hex')),
                ('2025-01-20 00:01:00+00', 2, decode('68656c6c6f', 'hex'))
            """
        )
    db.commit()
    partition_name = db.execute(
        """
        SELECT partition_name
        FROM deltax.deltax_partition_info('bytea_fallback')
        WHERE range_start = '2025-01-20 00:00:00+00'
        """
    ).fetchone()[0]
    db.execute("SELECT deltax.deltax_compress_partition(%s)", (partition_name,))
    assert_query_case(
        db,
        QueryCase(
            "bytea_fallback_projection",
            """
            SELECT id, encode(bytea_val, 'hex')
            FROM {table}
            ORDER BY id
            """,
        ),
        plain_table="bytea_fallback_plain",
        deltax_table="bytea_fallback",
    )


def test_decompress_recompress_round_trip_matches_plain_postgres(db):
    db.execute("SET pg_deltax.mock_now = '2025-01-20 12:00:00+00'")
    for table_name in ("round_trip_plain", "round_trip"):
        db.execute(
            f"""
            CREATE TABLE {table_name} (
                ts timestamptz NOT NULL,
                id integer NOT NULL,
                device_id integer,
                payload text,
                val integer,
                metric double precision
            )
            """
        )
    db.execute("SELECT deltax.deltax_create_table('round_trip', 'ts', '1 day'::interval, 3)")
    db.execute(
        "SELECT deltax.deltax_enable_compression("
        "'round_trip', segment_by => ARRAY['device_id'], "
        "order_by => ARRAY['ts', 'id'], segment_size => 3)"
    )
    db.commit()
    insert_sql = """
        INSERT INTO {table} VALUES
            ('2025-01-20 00:00:00+00', 1, 1, 'alpha', 10, 1.5),
            ('2025-01-20 00:10:00+00', 2, 1, 'beta', NULL, -2.25),
            ('2025-01-20 00:20:00+00', 3, 2, 'gamma', 30, NULL),
            ('2025-01-20 00:30:00+00', 4, 2, NULL, 40, 0.0)
    """
    db.execute(insert_sql.format(table="round_trip_plain"))
    db.execute(insert_sql.format(table="round_trip"))
    db.commit()
    partition_name = db.execute(
        """
        SELECT partition_name
        FROM deltax.deltax_partition_info('round_trip')
        WHERE range_start = '2025-01-20 00:00:00+00'
        """
    ).fetchone()[0]

    db.execute("SELECT deltax.deltax_compress_partition(%s)", (partition_name,))
    db.execute("SELECT deltax.deltax_decompress_partition(%s)", (partition_name,))
    assert_query_case(
        db,
        QueryCase(
            "after_decompress_projection",
            """
            SELECT id, ts, device_id, payload, val, metric
            FROM {table}
            ORDER BY ts, id
            """,
            comparator="float_tolerant",
        ),
        plain_table="round_trip_plain",
        deltax_table="round_trip",
    )

    db.execute("SELECT deltax.deltax_compress_partition(%s)", (partition_name,))
    assert_query_case(
        db,
        QueryCase(
            "after_recompress_projection",
            """
            SELECT id, ts, device_id, payload, val, metric
            FROM {table}
            ORDER BY ts, id
            """,
            comparator="float_tolerant",
        ),
        plain_table="round_trip_plain",
        deltax_table="round_trip",
    )


def test_mixed_regular_and_direct_backfill_partitions_match_plain_postgres(db):
    db.execute("SET pg_deltax.mock_now = '2025-01-20 12:00:00+00'")
    for table_name in ("mixed_load_paths_plain", "mixed_load_paths"):
        db.execute(
            f"""
            CREATE TABLE {table_name} (
                ts timestamptz NOT NULL,
                id integer NOT NULL,
                device_id integer,
                payload text,
                val integer
            )
            """
        )
    db.execute("SELECT deltax.deltax_create_table('mixed_load_paths', 'ts', '1 day'::interval, 3)")
    db.execute(
        "SELECT deltax.deltax_enable_compression("
        "'mixed_load_paths', segment_by => ARRAY['device_id'], "
        "order_by => ARRAY['ts', 'id'], segment_size => 3)"
    )
    db.commit()

    direct_rows = (
        ("2025-01-20 00:00:00+00", 1, 1, "direct-a", 10),
        ("2025-01-20 00:10:00+00", 2, 1, "direct-b", 20),
        ("2025-01-20 00:20:00+00", 3, 1, "direct-null", None),
    )
    regular_rows = (
        ("2025-01-21 00:00:00+00", 4, 2, "regular-a", 30),
        ("2025-01-21 00:10:00+00", 5, 2, "regular-b", 40),
        ("2025-01-21 00:20:00+00", 6, 2, "regular-null", None),
    )
    _copy_csv(db, "mixed_load_paths_plain", direct_rows + regular_rows, copy_format="csv")
    _copy_csv(db, "mixed_load_paths", direct_rows, copy_format="deltax_compress_csv")
    _copy_csv(db, "mixed_load_paths", regular_rows, copy_format="csv")
    db.commit()

    partition_name = db.execute(
        """
        SELECT partition_name
        FROM deltax.deltax_partition_info('mixed_load_paths')
        WHERE range_start = '2025-01-21 00:00:00+00'
        """
    ).fetchone()[0]
    db.execute("SELECT deltax.deltax_compress_partition(%s)", (partition_name,))
    db.execute("ANALYZE mixed_load_paths_plain")
    db.execute("ANALYZE mixed_load_paths")

    for case in (
        QueryCase(
            "mixed_load_paths_projection",
            """
            SELECT id, ts, device_id, payload, val
            FROM {table}
            ORDER BY ts, id
            """,
        ),
        QueryCase(
            "mixed_load_paths_grouped",
            """
            SELECT device_id, count(*), sum(val), min(payload), max(payload)
            FROM {table}
            GROUP BY device_id
            ORDER BY device_id NULLS LAST
            """,
        ),
    ):
        assert_query_case(
            db,
            case,
            plain_table="mixed_load_paths_plain",
            deltax_table="mixed_load_paths",
        )
