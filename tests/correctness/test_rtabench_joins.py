"""RTABench-shaped join correctness coverage."""

import pytest

from .datasets import create_rtabench_synthetic_pair
from .harness import assert_query_case
from .querygen import rtabench_synthetic_cases


pytestmark = pytest.mark.smoke


RTABENCH_LAYOUTS = (
    ("copy_text_s7", "copy_text", 7, False),
    ("copy_text_s25", "copy_text", 25, False),
    ("copy_csv_s7", "copy_csv", 7, False),
    ("copy_csv_s25", "copy_csv", 25, False),
)


@pytest.fixture(params=RTABENCH_LAYOUTS, ids=lambda layout: layout[0])
def rtabench_synthetic(db, request):
    layout_name, load_path, segment_size, mixed_uncompressed_tail = request.param
    plain_table, deltax_table = create_rtabench_synthetic_pair(
        db,
        deltax_table=f"rtabench_events_{layout_name}",
        load_path=load_path,
        segment_size=segment_size,
        mixed_uncompressed_tail=mixed_uncompressed_tail,
    )
    return plain_table, deltax_table, layout_name, mixed_uncompressed_tail


@pytest.mark.parametrize("case", list(rtabench_synthetic_cases()), ids=lambda case: case.name)
def test_rtabench_synthetic_joins_match_plain_postgres(rtabench_synthetic, db, case):
    plain_table, deltax_table, _, _ = rtabench_synthetic
    assert_query_case(
        db,
        case,
        plain_table=plain_table,
        deltax_table=deltax_table,
    )


def test_rtabench_synthetic_compression_happened(rtabench_synthetic, db):
    _, deltax_table, _, _ = rtabench_synthetic
    compressed = db.execute(
        f"""
        SELECT count(*)
        FROM deltax.deltax_compression_stats('{deltax_table}')
        WHERE is_compressed = true
          AND row_count > 0
        """
    ).fetchone()[0]
    assert compressed > 0


JOIN_STRATEGY_CASES = (
    "dimension_join_fact_inner",
    "full_outer_join_product_event_coverage",
    "range_join_events_after_order_created",
)

JOIN_STRATEGY_SETTINGS = (
    "enable_hashjoin",
    "enable_mergejoin",
    "enable_nestloop",
)


def _rtabench_case(name: str):
    for case in rtabench_synthetic_cases():
        if case.name == name:
            return case
    raise AssertionError(f"unknown RTABench correctness case: {name}")


@pytest.mark.parametrize(
    ("layout_name", "load_path", "segment_size"),
    (
        ("copy_text_mixed_s7", "copy_text", 7),
        ("copy_csv_mixed_s25", "copy_csv", 25),
    ),
)
def test_rtabench_compressed_partition_rejects_parent_routed_insert(
    db,
    layout_name,
    load_path,
    segment_size,
):
    plain_table, deltax_table = create_rtabench_synthetic_pair(
        db,
        deltax_table=f"rtabench_events_{layout_name}",
        load_path=load_path,
        segment_size=segment_size,
    )
    with pytest.raises(Exception, match="compressed partition"):
        db.execute(
            f"""
            INSERT INTO {deltax_table} (
                order_id,
                counter,
                event_created,
                event_type,
                satisfaction,
                processor,
                backup_processor,
                event_payload
            )
            VALUES (
                181,
                9001,
                '2024-05-04 12:00:00+00',
                'Delivered',
                4.5,
                'proc-a',
                'proc-b',
                '{{}}'::jsonb
            )
            """
        )
    db.rollback()

    assert_query_case(
        db,
        _rtabench_case("dimension_join_fact_inner"),
        plain_table=plain_table,
        deltax_table=deltax_table,
    )


@pytest.mark.parametrize(
    ("operation", "sql_template"),
    (
        (
            "update",
            """
            UPDATE {table}
            SET satisfaction = satisfaction + 0.25
            WHERE order_id = 1
            """,
        ),
        (
            "delete",
            """
            DELETE FROM {table}
            WHERE order_id = 1
            """,
        ),
    ),
)
def test_rtabench_compressed_partition_rejects_parent_routed_update_delete(
    rtabench_synthetic,
    db,
    operation,
    sql_template,
):
    _, deltax_table, _, _ = rtabench_synthetic
    with pytest.raises(Exception, match="compressed partition"):
        db.execute(sql_template.format(table=deltax_table))
    db.rollback()


@pytest.mark.parametrize("case", [c for c in rtabench_synthetic_cases() if c.name in JOIN_STRATEGY_CASES], ids=lambda case: case.name)
@pytest.mark.parametrize("disabled_join", JOIN_STRATEGY_SETTINGS)
def test_rtabench_join_strategies_match_plain_postgres(
    rtabench_synthetic,
    db,
    case,
    disabled_join,
):
    plain_table, deltax_table, _, _ = rtabench_synthetic
    db.execute(f"SET {disabled_join} = off")
    try:
        assert_query_case(
            db,
            case,
            plain_table=plain_table,
            deltax_table=deltax_table,
        )
    finally:
        db.execute(f"RESET {disabled_join}")


PREPARED_JOIN_PARAMS = (
    ("Delivered", 1, 55, "2024-05-01 00:00:00+00", "2024-05-08 00:00:00+00"),
    ("Returned", 20, 120, "2024-05-03 00:00:00+00", "2024-05-13 00:00:00+00"),
    ("Cancelled", 60, 180, "2024-05-04 00:00:00+00", "2024-05-15 00:00:00+00"),
    ("Created", 1, 180, "2024-05-01 00:00:00+00", "2024-05-15 00:00:00+00"),
)


def test_rtabench_prepared_parameterized_join_matches_plain_postgres(rtabench_synthetic, db):
    plain_table, deltax_table, _, _ = rtabench_synthetic
    db.execute(
        f"""
        PREPARE plain_rtabench_join(text, integer, integer, timestamptz, timestamptz) AS
        SELECT o.customer_id, oe.event_type, count(*) AS events, max(oe.event_created) AS latest_event
        FROM orders o
        JOIN {plain_table} oe ON oe.order_id = o.order_id
        WHERE oe.event_type = $1
          AND oe.order_id BETWEEN $2 AND $3
          AND oe.event_created >= $4
          AND oe.event_created < $5
        GROUP BY o.customer_id, oe.event_type
        ORDER BY o.customer_id, oe.event_type
        """
    )
    db.execute(
        f"""
        PREPARE deltax_rtabench_join(text, integer, integer, timestamptz, timestamptz) AS
        SELECT o.customer_id, oe.event_type, count(*) AS events, max(oe.event_created) AS latest_event
        FROM orders o
        JOIN {deltax_table} oe ON oe.order_id = o.order_id
        WHERE oe.event_type = $1
          AND oe.order_id BETWEEN $2 AND $3
          AND oe.event_created >= $4
          AND oe.event_created < $5
        GROUP BY o.customer_id, oe.event_type
        ORDER BY o.customer_id, oe.event_type
        """
    )

    for params in PREPARED_JOIN_PARAMS:
        event_type, lo, hi, ts_lo, ts_hi = params
        plain_rows = db.execute(
            f"""
            EXECUTE plain_rtabench_join(
                '{event_type}',
                {lo},
                {hi},
                '{ts_lo}'::timestamptz,
                '{ts_hi}'::timestamptz
            )
            """
        ).fetchall()
        deltax_rows = db.execute(
            f"""
            EXECUTE deltax_rtabench_join(
                '{event_type}',
                {lo},
                {hi},
                '{ts_lo}'::timestamptz,
                '{ts_hi}'::timestamptz
            )
            """
        ).fetchall()
        assert plain_rows == deltax_rows, params
