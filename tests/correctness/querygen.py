"""Seeded query generation helpers.

This module is intentionally a placeholder for the next phase. The initial
correctness suite is curated; generated cases should build QueryCase instances
here and keep their seed in the test id / failure output.
"""

from __future__ import annotations

from collections.abc import Iterable

from .harness import QueryCase


def curated_smoke_cases() -> Iterable[QueryCase]:
    yield QueryCase(
        "count_all",
        "SELECT count(*) FROM {table}",
    )
    yield QueryCase(
        "filtered_projection",
        """
        SELECT id, device_id, kind, val
        FROM {table}
        WHERE ts >= '2025-01-15 00:10:00+00'
          AND ts < '2025-01-15 01:10:00+00'
          AND (kind = 'alpha' OR device_id IS NULL)
        ORDER BY ts, id
        """,
    )
    yield QueryCase(
        "grouped_aggregate",
        """
        SELECT device_id, kind, count(*), sum(val), min(val), max(val)
        FROM {table}
        GROUP BY device_id, kind
        ORDER BY device_id NULLS LAST, kind NULLS LAST
        """,
    )
    yield QueryCase(
        "deterministic_topn",
        """
        SELECT id, ts, kind, val
        FROM {table}
        ORDER BY val DESC NULLS LAST, id
        LIMIT 10
        """,
    )


def predicate_matrix_cases() -> Iterable[QueryCase]:
    order = "ORDER BY id"

    yield QueryCase(
        "eq_integer",
        f"SELECT id, int_val FROM {{table}} WHERE int_val = 7 {order}",
    )
    yield QueryCase(
        "neq_integer",
        f"SELECT id, int_val FROM {{table}} WHERE int_val <> -3 {order}",
    )
    yield QueryCase(
        "integer_in",
        f"SELECT id, int_val FROM {{table}} WHERE int_val IN (-12, 0, 12) {order}",
    )
    yield QueryCase(
        "integer_not_in",
        f"SELECT id, int_val FROM {{table}} WHERE int_val NOT IN (-12, 0, 12) {order}",
    )
    yield QueryCase(
        "integer_no_matches",
        f"SELECT id, int_val FROM {{table}} WHERE int_val = 999 {order}",
    )
    yield QueryCase(
        "range_lt_lte",
        f"SELECT id, int_val FROM {{table}} WHERE int_val < -10 OR int_val <= 3 {order}",
    )
    yield QueryCase(
        "range_gt_gte",
        f"SELECT id, int_val FROM {{table}} WHERE int_val > 12 OR int_val >= 18 {order}",
    )
    yield QueryCase(
        "between_integer",
        f"SELECT id, int_val FROM {{table}} WHERE int_val BETWEEN -4 AND 4 {order}",
    )
    yield QueryCase(
        "not_between_integer",
        f"SELECT id, int_val FROM {{table}} WHERE int_val NOT BETWEEN -4 AND 4 {order}",
    )
    yield QueryCase(
        "same_column_or_range",
        f"SELECT id, int_val FROM {{table}} WHERE int_val < -15 OR int_val > 15 {order}",
    )
    yield QueryCase(
        "timestamp_range",
        """
        SELECT id, ts
        FROM {table}
        WHERE ts >= '2025-01-15 03:00:00+00'
          AND ts < '2025-01-15 07:00:00+00'
        ORDER BY id
        """,
    )
    yield QueryCase(
        "timestamp_lte",
        f"SELECT id, ts FROM {{table}} WHERE ts <= '2025-01-15 02:00:00+00' {order}",
    )
    yield QueryCase(
        "timestamp_gt",
        f"SELECT id, ts FROM {{table}} WHERE ts > '2025-01-15 09:30:00+00' {order}",
    )
    yield QueryCase(
        "float_eq",
        f"SELECT id, score FROM {{table}} WHERE score = 1.0 {order}",
    )
    yield QueryCase(
        "float_neq",
        f"SELECT id, score FROM {{table}} WHERE score <> -1.0 {order}",
    )
    yield QueryCase(
        "float_range",
        f"SELECT id, score FROM {{table}} WHERE score >= -2.5 AND score < 3.25 {order}",
    )
    yield QueryCase(
        "float_between",
        f"SELECT id, score FROM {{table}} WHERE score BETWEEN -2.5 AND 3.25 {order}",
    )
    yield QueryCase(
        "is_null",
        f"SELECT id, device_id, int_val FROM {{table}} WHERE device_id IS NULL {order}",
    )
    yield QueryCase(
        "is_not_null",
        f"SELECT id, low_text FROM {{table}} WHERE low_text IS NOT NULL {order}",
    )
    yield QueryCase(
        "segment_by_eq",
        f"SELECT id, device_id FROM {{table}} WHERE device_id = 3 {order}",
    )
    yield QueryCase(
        "segment_by_neq",
        f"SELECT id, device_id FROM {{table}} WHERE device_id <> 3 {order}",
    )
    yield QueryCase(
        "is_distinct_from",
        f"SELECT id, int_val FROM {{table}} WHERE int_val IS DISTINCT FROM 7 {order}",
    )
    yield QueryCase(
        "is_not_distinct_from",
        f"SELECT id, int_val FROM {{table}} WHERE int_val IS NOT DISTINCT FROM NULL {order}",
    )
    yield QueryCase(
        "in_with_null_data",
        f"SELECT id, low_text FROM {{table}} WHERE low_text IN ('red', 'green') {order}",
    )
    yield QueryCase(
        "not_in_without_null_literal",
        f"SELECT id, low_text FROM {{table}} WHERE low_text NOT IN ('red', 'green') {order}",
    )
    yield QueryCase(
        "not_in_with_null_literal",
        f"SELECT id, int_val FROM {{table}} WHERE int_val NOT IN (-1, 0, NULL) {order}",
    )
    yield QueryCase(
        "like_prefix",
        f"SELECT id, high_text FROM {{table}} WHERE high_text LIKE 'prefix-%' {order}",
    )
    yield QueryCase(
        "like_contains",
        f"SELECT id, high_text FROM {{table}} WHERE high_text LIKE '%contains' {order}",
    )
    yield QueryCase(
        "not_like",
        f"SELECT id, high_text FROM {{table}} WHERE high_text NOT LIKE 'token-%' {order}",
    )
    yield QueryCase(
        "boolean_true",
        f"SELECT id, active FROM {{table}} WHERE active = true {order}",
    )
    yield QueryCase(
        "boolean_is_not_true",
        f"SELECT id, active FROM {{table}} WHERE active IS NOT TRUE {order}",
    )
    yield QueryCase(
        "nested_boolean_logic",
        """
        SELECT id, device_id, low_text, int_val, active
        FROM {table}
        WHERE (
            (low_text = 'red' AND int_val BETWEEN -10 AND 10)
            OR (device_id IS NULL AND NOT (active IS TRUE))
            OR (high_text LIKE 'prefix-%' AND score > 1.0)
        )
        ORDER BY id
        """,
    )
    yield QueryCase(
        "cast_text_to_int",
        f"SELECT id, code FROM {{table}} WHERE code::integer BETWEEN 110 AND 125 {order}",
    )
    yield QueryCase(
        "timestamp_date_cast",
        f"SELECT id, ts FROM {{table}} WHERE ts::date = DATE '2025-01-15' {order}",
    )


def ordering_topn_cases() -> Iterable[QueryCase]:
    yield QueryCase(
        "ts_asc_limit",
        """
        SELECT id, ts, payload
        FROM {table}
        ORDER BY ts ASC, id ASC
        LIMIT 15
        """,
    )
    yield QueryCase(
        "ts_desc_limit",
        """
        SELECT id, ts, payload
        FROM {table}
        ORDER BY ts DESC, id DESC
        LIMIT 15
        """,
    )
    yield QueryCase(
        "val_asc_nulls_first",
        """
        SELECT id, sort_val, payload
        FROM {table}
        ORDER BY sort_val ASC NULLS FIRST, id ASC
        LIMIT 20
        """,
    )
    yield QueryCase(
        "val_desc_nulls_last",
        """
        SELECT id, sort_val, payload
        FROM {table}
        ORDER BY sort_val DESC NULLS LAST, id ASC
        LIMIT 20
        """,
    )
    yield QueryCase(
        "multi_column_order",
        """
        SELECT id, device_id, sort_val, ts, tie_val
        FROM {table}
        ORDER BY device_id NULLS LAST, sort_val NULLS FIRST, ts DESC, tie_val, id
        LIMIT 25
        """,
    )
    yield QueryCase(
        "limit_offset",
        """
        SELECT id, ts, payload
        FROM {table}
        ORDER BY ts ASC, id ASC
        LIMIT 12 OFFSET 9
        """,
    )
    yield QueryCase(
        "filtered_non_sort_key",
        """
        SELECT id, sort_val, active, payload
        FROM {table}
        WHERE active IS TRUE AND payload LIKE 'alpha-%'
        ORDER BY sort_val ASC NULLS LAST, id DESC
        LIMIT 12
        """,
    )
    yield QueryCase(
        "project_extra_after_topn",
        """
        SELECT id, ts, sort_val, payload, extra, metric
        FROM {table}
        ORDER BY ts DESC, id DESC
        LIMIT 14
        """,
    )
    yield QueryCase(
        "topn_float_sort",
        """
        SELECT id, metric, payload
        FROM {table}
        WHERE metric IS NOT NULL
        ORDER BY metric ASC, id ASC
        LIMIT 18
        """,
    )
    yield QueryCase(
        "non_unique_limit_ties",
        """
        SELECT id, sort_val
        FROM {table}
        ORDER BY sort_val ASC NULLS LAST
        LIMIT 10
        """,
        comparator="limit_ties",
    )
