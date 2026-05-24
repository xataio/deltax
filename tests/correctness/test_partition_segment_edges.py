"""Partition and segment edge correctness coverage."""

import pytest

from .datasets import (
    create_partition_segment_edges_direct_backfill_pair,
    create_partition_segment_edges_pair,
)
from .harness import QueryCase, assert_query_case
from .querygen import (
    partition_segment_boundary_cases,
    partition_segment_direct_backfill_cases,
    partition_segment_edge_cases,
    partition_segment_fastpath_cases,
    partition_segment_plan_shape_cases,
)


pytestmark = pytest.mark.smoke

FASTPATH_MODES = (
    ("fastpath_on", "off"),
    ("fastpath_disabled", "on"),
)


@pytest.fixture()
def partition_segment_edges(db):
    return create_partition_segment_edges_pair(db)


@pytest.fixture(params=(1, 2, 5), ids=lambda size: f"segment_size_{size}")
def partition_segment_edges_by_segment_size(db, request):
    return create_partition_segment_edges_pair(
        db,
        deltax_table=f"partition_segment_edges_s{request.param}",
        segment_size=request.param,
    )


@pytest.fixture()
def direct_backfill_partition_segment_edges(db):
    return create_partition_segment_edges_direct_backfill_pair(db)


@pytest.mark.parametrize("case", list(partition_segment_edge_cases()), ids=lambda case: case.name)
def test_partition_segment_edges_match_plain_postgres(partition_segment_edges, db, case):
    plain_table, deltax_table = partition_segment_edges
    assert_query_case(
        db,
        case,
        plain_table=plain_table,
        deltax_table=deltax_table,
    )


@pytest.mark.parametrize(
    "case",
    list(partition_segment_boundary_cases()),
    ids=lambda case: case.name,
)
def test_partition_boundary_variants_match_plain_postgres(partition_segment_edges, db, case):
    plain_table, deltax_table = partition_segment_edges
    assert_query_case(
        db,
        case,
        plain_table=plain_table,
        deltax_table=deltax_table,
    )


@pytest.mark.parametrize(
    "case",
    list(partition_segment_plan_shape_cases()),
    ids=lambda case: case.name,
)
def test_partition_plan_shapes_match_plain_postgres(partition_segment_edges, db, case):
    plain_table, deltax_table = partition_segment_edges
    assert_query_case(
        db,
        case,
        plain_table=plain_table,
        deltax_table=deltax_table,
    )


@pytest.mark.parametrize(
    "case",
    [
        QueryCase(
            "all_rows_ordered",
            """
            SELECT id, ts, bucket, payload
            FROM {table}
            WHERE ts >= '2025-01-14 00:00:00+00'
              AND ts < '2025-01-18 00:00:00+00'
            ORDER BY ts, id
            """,
        ),
        QueryCase(
            "filtered_boundary_topn",
            """
            SELECT id, ts, bucket, val, payload
            FROM {table}
            WHERE val IS NULL OR val >= 35
            ORDER BY ts DESC, id DESC
            LIMIT 8
            """,
        ),
    ],
    ids=lambda case: case.name,
)
def test_partition_segment_size_variants_match_plain_postgres(
    partition_segment_edges_by_segment_size,
    db,
    case,
):
    plain_table, deltax_table = partition_segment_edges_by_segment_size
    assert_query_case(
        db,
        case,
        plain_table=plain_table,
        deltax_table=deltax_table,
    )


@pytest.mark.parametrize(
    "case",
    list(partition_segment_direct_backfill_cases()),
    ids=lambda case: case.name,
)
def test_direct_backfill_partition_segment_edges_match_plain_postgres(
    direct_backfill_partition_segment_edges,
    db,
    case,
):
    plain_table, deltax_table = direct_backfill_partition_segment_edges
    assert_query_case(
        db,
        case,
        plain_table=plain_table,
        deltax_table=deltax_table,
    )


@pytest.mark.parametrize(
    "case",
    list(partition_segment_fastpath_cases()),
    ids=lambda case: case.name,
)
@pytest.mark.parametrize("fastpath_mode", FASTPATH_MODES, ids=lambda mode: mode[0])
def test_partition_segment_fastpath_modes_match_plain_postgres(
    partition_segment_edges,
    db,
    case,
    fastpath_mode,
):
    _, guc_value = fastpath_mode
    plain_table, deltax_table = partition_segment_edges
    db.execute(f"SET pg_deltax.disable_meta_agg_fastpath = {guc_value}")
    assert_query_case(
        db,
        case,
        plain_table=plain_table,
        deltax_table=deltax_table,
    )


def test_prepared_time_ranges_match_plain_postgres(partition_segment_edges, db):
    plain_table, deltax_table = partition_segment_edges
    ranges = (
        ("compressed_only", "2025-01-15 00:00:00+00", "2025-01-16 00:00:00+00"),
        ("uncompressed_only", "2025-01-16 00:00:00+00", "2025-01-17 00:00:00+00"),
        ("default_old_only", "2025-01-13 00:00:00+00", "2025-01-14 00:00:00+00"),
        ("mixed_registered", "2025-01-15 18:00:00+00", "2025-01-17 02:00:00+00"),
    )

    db.execute(
        f"""
        PREPARE plain_partition_range(timestamptz, timestamptz) AS
        SELECT id, ts, bucket, val, payload
        FROM {plain_table}
        WHERE ts >= $1 AND ts < $2
        ORDER BY ts, id
        """
    )
    db.execute(
        f"""
        PREPARE deltax_partition_range(timestamptz, timestamptz) AS
        SELECT id, ts, bucket, val, payload
        FROM {deltax_table}
        WHERE ts >= $1 AND ts < $2
        ORDER BY ts, id
        """
    )

    for range_name, lo, hi in ranges:
        plain_rows = db.execute(
            f"EXECUTE plain_partition_range('{lo}'::timestamptz, '{hi}'::timestamptz)"
        ).fetchall()
        deltax_rows = db.execute(
            f"EXECUTE deltax_partition_range('{lo}'::timestamptz, '{hi}'::timestamptz)"
        ).fetchall()
        assert plain_rows == deltax_rows, range_name


def test_partition_edge_join_matches_plain_postgres(partition_segment_edges, db):
    plain_table, deltax_table = partition_segment_edges
    db.execute(
        """
        CREATE TABLE edge_devices (
            device_id integer PRIMARY KEY,
            label text NOT NULL
        )
        """
    )
    db.execute(
        """
        INSERT INTO edge_devices (device_id, label)
        VALUES (0, 'zero'), (1, 'one'), (2, 'two')
        """
    )

    case = QueryCase(
        "partition_edge_join",
        """
        SELECT t.id, t.ts, t.bucket, d.label, t.payload
        FROM {table} t
        JOIN edge_devices d ON d.device_id = t.device_id
        WHERE t.ts >= '2025-01-15 18:00:00+00'
          AND t.ts < '2025-01-19 00:00:00+00'
        ORDER BY t.ts, t.id
        """,
    )
    assert_query_case(
        db,
        case,
        plain_table=plain_table,
        deltax_table=deltax_table,
    )


def test_partition_parent_only_has_no_rows(partition_segment_edges, db):
    _, deltax_table = partition_segment_edges
    parent_rows = db.execute(f"SELECT count(*) FROM ONLY {deltax_table}").fetchone()[0]
    all_rows = db.execute(f"SELECT count(*) FROM {deltax_table}").fetchone()[0]
    assert parent_rows == 0
    assert all_rows > 0


@pytest.mark.parametrize(
    "statement",
    (
        "UPDATE {partition} SET val = 999 WHERE id = 1",
        "DELETE FROM {partition} WHERE id = 1",
    ),
    ids=("update", "delete"),
)
def test_partition_edge_compressed_partition_rejects_dml(partition_segment_edges, db, statement):
    _, deltax_table = partition_segment_edges
    partition_name = db.execute(
        f"""
        SELECT partition_name
        FROM deltax.deltax_partition_info('{deltax_table}')
        WHERE is_compressed
        ORDER BY partition_name
        LIMIT 1
        """
    ).fetchone()[0]

    with pytest.raises(Exception, match="cannot .* compressed partition"):
        db.execute(statement.format(partition=partition_name))
    db.rollback()


def test_partition_edges_match_plain_in_non_utc_session_timezone(partition_segment_edges, db):
    plain_table, deltax_table = partition_segment_edges
    db.execute("SET TIME ZONE 'Europe/Berlin'")
    case = QueryCase(
        "non_utc_session_timezone_half_open_boundary",
        """
        SELECT id, ts, bucket, payload
        FROM {table}
        WHERE ts >= '2025-01-15 01:00:00+01'::timestamptz
          AND ts < '2025-01-16 01:00:00+01'::timestamptz
        ORDER BY ts, id
        """,
    )
    assert_query_case(
        db,
        case,
        plain_table=plain_table,
        deltax_table=deltax_table,
    )


def test_two_day_partition_interval_boundaries_match_plain_postgres(db):
    db.execute("SET pg_deltax.mock_now = '2025-01-20 12:00:00+00'")
    for table_name in ("two_day_edges_plain", "two_day_edges"):
        db.execute(
            f"""
            CREATE TABLE {table_name} (
                ts timestamptz NOT NULL,
                id integer NOT NULL,
                bucket text,
                val integer
            )
            """
        )
    db.execute("SELECT deltax.deltax_create_table('two_day_edges', 'ts', '2 days'::interval, 3)")
    db.execute(
        "SELECT deltax.deltax_enable_compression("
        "'two_day_edges', segment_by => ARRAY['bucket'], "
        "order_by => ARRAY['ts', 'id'], segment_size => 3)"
    )
    db.commit()

    insert_sql = """
        INSERT INTO {table} (ts, id, bucket, val)
        VALUES
            ('2025-01-14 00:00:00+00', 1, 'start', 10),
            ('2025-01-15 23:59:59.999999+00', 2, 'end_minus_epsilon', 20),
            ('2025-01-16 00:00:00+00', 3, 'next_start', 30),
            ('2025-01-17 12:00:00+00', 4, 'middle', 40),
            ('2025-01-18 00:00:00+00', 5, 'second_next_start', 50)
    """
    db.execute(insert_sql.format(table="two_day_edges_plain"))
    db.execute(insert_sql.format(table="two_day_edges"))
    db.commit()

    partitions = db.execute(
        """
        SELECT partition_name
        FROM deltax.deltax_partition_info('two_day_edges')
        WHERE range_start >= '2025-01-14 00:00:00+00'
          AND range_start < '2025-01-18 00:00:00+00'
        ORDER BY partition_name
        """
    ).fetchall()
    for (partition_name,) in partitions:
        db.execute("SELECT deltax.deltax_compress_partition(%s)", (partition_name,))
    db.execute("ANALYZE two_day_edges_plain")
    db.execute("ANALYZE two_day_edges")

    for case in (
        QueryCase(
            "two_day_first_partition",
            """
            SELECT id, ts, bucket, val
            FROM {table}
            WHERE ts >= '2025-01-14 00:00:00+00'
              AND ts < '2025-01-16 00:00:00+00'
            ORDER BY ts, id
            """,
        ),
        QueryCase(
            "two_day_cross_boundary",
            """
            SELECT id, ts, bucket, val
            FROM {table}
            WHERE ts >= '2025-01-15 23:59:59.999999+00'
              AND ts <= '2025-01-18 00:00:00+00'
            ORDER BY ts, id
            """,
        ),
    ):
        assert_query_case(
            db,
            case,
            plain_table="two_day_edges_plain",
            deltax_table="two_day_edges",
        )
