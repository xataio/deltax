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
    yield QueryCase(
        "limit_zero_empty_startup",
        """
        SELECT id, ts, device_id, kind, val, metric
        FROM {table}
        ORDER BY ts, id
        LIMIT 0
        """,
        comparator="float_tolerant",
    )
    yield QueryCase(
        "empty_result_projection",
        """
        SELECT id, ts, device_id, kind, val, metric
        FROM {table}
        WHERE ts >= '2030-01-01 00:00:00+00'
        ORDER BY ts, id
        """,
        comparator="float_tolerant",
    )
    yield QueryCase(
        "full_row_projection",
        """
        SELECT *
        FROM {table}
        WHERE id BETWEEN 7 AND 23 OR device_id IS NULL
        ORDER BY ts, id
        """,
        comparator="float_tolerant",
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
        "ilike_prefix",
        f"SELECT id, high_text FROM {{table}} WHERE high_text ILIKE 'PREFIX-%' {order}",
    )
    yield QueryCase(
        "similar_to_text",
        f"SELECT id, high_text FROM {{table}} WHERE high_text SIMILAR TO '(prefix|middle)-%' {order}",
    )
    yield QueryCase(
        "regex_match",
        f"SELECT id, high_text FROM {{table}} WHERE high_text ~ '^(prefix|token)-0[0-9]{{{{2}}}}' {order}",
    )
    yield QueryCase(
        "regex_not_match",
        f"SELECT id, high_text FROM {{table}} WHERE high_text !~ 'contains$' {order}",
    )
    yield QueryCase(
        "boolean_true",
        f"SELECT id, active FROM {{table}} WHERE active = true {order}",
    )
    yield QueryCase(
        "boolean_is_true",
        f"SELECT id, active FROM {{table}} WHERE active IS TRUE {order}",
    )
    yield QueryCase(
        "boolean_is_false",
        f"SELECT id, active FROM {{table}} WHERE active IS FALSE {order}",
    )
    yield QueryCase(
        "boolean_is_unknown",
        f"SELECT id, active FROM {{table}} WHERE active IS UNKNOWN {order}",
    )
    yield QueryCase(
        "boolean_is_not_true",
        f"SELECT id, active FROM {{table}} WHERE active IS NOT TRUE {order}",
    )
    yield QueryCase(
        "boolean_is_not_false",
        f"SELECT id, active FROM {{table}} WHERE active IS NOT FALSE {order}",
    )
    yield QueryCase(
        "scalar_array_any",
        f"SELECT id, int_val FROM {{table}} WHERE int_val = ANY(ARRAY[-20, -5, 0, 5, 20]) {order}",
    )
    yield QueryCase(
        "scalar_array_all_with_null",
        f"SELECT id, int_val FROM {{table}} WHERE int_val <> ALL(ARRAY[-1, 0, NULL]::integer[]) {order}",
    )
    yield QueryCase(
        "row_comparison_in",
        """
        SELECT id, int_val, device_id
        FROM {table}
        WHERE (int_val, device_id) IN ((-5, 1), (0, 2), (7, 3))
        ORDER BY id
        """,
    )
    yield QueryCase(
        "row_comparison_gt",
        """
        SELECT id, int_val, device_id
        FROM {table}
        WHERE (coalesce(int_val, -999), id) > (10, 40)
        ORDER BY id
        """,
    )
    yield QueryCase(
        "expression_vs_expression_predicate",
        """
        SELECT id, int_val, device_id, score
        FROM {table}
        WHERE coalesce(int_val, 0) + coalesce(device_id, 0) > floor(coalesce(score, 0.0))::integer
        ORDER BY id
        """,
        comparator="float_tolerant",
    )
    yield QueryCase(
        "date_trunc_expression_predicate",
        """
        SELECT id, ts
        FROM {table}
        WHERE date_trunc('hour', ts) < '2025-01-15 06:00:00+00'::timestamptz
        ORDER BY id
        """,
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
        "negated_large_predicate_tree",
        """
        SELECT id, device_id, low_text, int_val, active
        FROM {table}
        WHERE NOT (
            (low_text = 'red' AND int_val BETWEEN -5 AND 5)
            OR (device_id IS NULL AND active IS NOT TRUE)
            OR (high_text LIKE 'middle-%' AND score < 0.0)
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
        "mixed_direction_multi_column",
        """
        SELECT id, sort_val, ts, payload
        FROM {table}
        ORDER BY sort_val ASC NULLS LAST, ts DESC, id ASC
        LIMIT 20
        """,
    )
    yield QueryCase(
        "text_asc_nulls_first",
        """
        SELECT id, text_sort, payload
        FROM {table}
        ORDER BY text_sort ASC NULLS FIRST, id ASC
        LIMIT 20
        """,
    )
    yield QueryCase(
        "text_desc_nulls_last_late_projection",
        """
        SELECT id, text_sort, extra, metric
        FROM {table}
        ORDER BY text_sort DESC NULLS LAST, id DESC
        LIMIT 18
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
        "limit_zero",
        """
        SELECT id, ts, payload
        FROM {table}
        ORDER BY ts ASC, id ASC
        LIMIT 0
        """,
    )
    yield QueryCase(
        "offset_beyond_rows",
        """
        SELECT id, ts, payload
        FROM {table}
        ORDER BY ts ASC, id ASC
        LIMIT 15 OFFSET 10000
        """,
    )
    yield QueryCase(
        "limit_offset_no_rows",
        """
        SELECT id, sort_val, payload
        FROM {table}
        ORDER BY sort_val ASC NULLS LAST, id ASC
        LIMIT 5 OFFSET 10000
        """,
    )
    yield QueryCase(
        "fetch_first_with_ties",
        """
        SELECT id, sort_val
        FROM {table}
        ORDER BY sort_val ASC NULLS LAST
        FETCH FIRST 10 ROWS WITH TIES
        """,
        comparator="limit_ties",
    )
    yield QueryCase(
        "order_by_expression",
        """
        SELECT id, sort_val, text_sort, payload
        FROM {table}
        ORDER BY coalesce(sort_val, 999), lower(coalesce(text_sort, 'zzzz')), id
        LIMIT 24
        """,
    )
    yield QueryCase(
        "order_by_alias_and_ordinal",
        """
        SELECT sort_val AS s, id, payload
        FROM {table}
        ORDER BY s DESC NULLS LAST, 2 ASC
        LIMIT 19
        """,
    )
    yield QueryCase(
        "topn_subquery_outer_reorder",
        """
        SELECT id, ts, payload
        FROM (
            SELECT id, ts, payload, sort_val
            FROM {table}
            ORDER BY ts DESC, id DESC
            LIMIT 40
        ) picked
        ORDER BY sort_val NULLS LAST, id
        LIMIT 15
        """,
    )
    yield QueryCase(
        "distinct_on_order_limit",
        """
        SELECT DISTINCT ON (device_id) device_id, id, ts, payload
        FROM {table}
        WHERE device_id IS NOT NULL
        ORDER BY device_id, ts DESC, id DESC
        LIMIT 10
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
    yield QueryCase(
        "limit_above_topn_cap",
        """
        SELECT id, ts, payload
        FROM {table}
        ORDER BY ts ASC, id ASC
        LIMIT 10001
        """,
    )
    yield QueryCase(
        "non_constant_limit",
        """
        SELECT id, sort_val, payload
        FROM {table}
        ORDER BY sort_val ASC NULLS LAST, id ASC
        LIMIT (SELECT 17)
        """,
    )
    yield QueryCase(
        "aggregate_order_limit",
        """
        SELECT active, count(*), min(sort_val), max(sort_val)
        FROM {table}
        GROUP BY active
        ORDER BY count(*) DESC, active NULLS LAST
        LIMIT 3
        """,
    )


def collation_topn_cases() -> Iterable[QueryCase]:
    """Top-N text cases for the byte-order collation fast path.

    Run against a table whose text columns carry a specific collation (see
    ``create_collation_edges_pair``). Every result is compared to plain
    PostgreSQL under the *same* collation, so any place the fast path uses byte
    order where the collation is linguistic — or honours the column collation
    instead of a query-level ``COLLATE`` override — shows up as a mismatch.
    """
    # Single-column, distinct sort key: exercises the exact-limit path. Under a
    # byte-order collation it takes byte comparison; otherwise strcoll. Distinct
    # values make the top-N total, so no tie ambiguity.
    yield QueryCase(
        "sort_text_asc",
        "SELECT id, sort_text FROM {table} ORDER BY sort_text ASC LIMIT 20",
    )
    yield QueryCase(
        "sort_text_desc",
        "SELECT id, sort_text FROM {table} ORDER BY sort_text DESC LIMIT 20",
    )
    # Low-cardinality first key + unique tiebreaker: multi-column margin path
    # (rows tied on dup_text must survive worker truncation to be ordered by id).
    yield QueryCase(
        "dup_text_multi_asc",
        "SELECT id, dup_text FROM {table} ORDER BY dup_text ASC, id ASC LIMIT 25",
    )
    yield QueryCase(
        "dup_text_multi_desc",
        "SELECT id, dup_text FROM {table} ORDER BY dup_text DESC, id DESC LIMIT 25",
    )
    # NULL placement through the Top-N path.
    yield QueryCase(
        "opt_text_nulls_first",
        "SELECT id, opt_text FROM {table} "
        "ORDER BY opt_text ASC NULLS FIRST, id ASC LIMIT 20",
    )
    yield QueryCase(
        "opt_text_nulls_last",
        "SELECT id, opt_text FROM {table} "
        "ORDER BY opt_text DESC NULLS LAST, id ASC LIMIT 20",
    )
    # Query-level COLLATE overrides: the sort collation differs from the
    # column's declared collation. On a byte-order-collated table the "unicode"
    # override forces a linguistic sort; on an ICU-collated table the "C"
    # override forces byte order. These probe whether the fast path keys off the
    # column collation (a bug) or the actual sort collation.
    yield QueryCase(
        "override_force_c_multi",
        'SELECT id, sort_text FROM {table} '
        'ORDER BY sort_text COLLATE "C" ASC, id ASC LIMIT 20',
    )
    yield QueryCase(
        "override_force_icu_multi",
        'SELECT id, sort_text FROM {table} '
        'ORDER BY sort_text COLLATE "unicode" ASC, id ASC LIMIT 20',
    )
    # Single-column override (distinct key → deterministic): the riskiest shape,
    # since it can reach the exact-limit path with a mismatched collation.
    yield QueryCase(
        "override_force_icu_single",
        'SELECT id, sort_text FROM {table} '
        'ORDER BY sort_text COLLATE "unicode" LIMIT 20',
    )


def aggregate_matrix_cases() -> Iterable[QueryCase]:
    yield QueryCase(
        "count_star_and_count_col",
        """
        SELECT count(*), count(int_nullable), count(all_null_input)
        FROM {table}
        """,
    )
    yield QueryCase(
        "numeric_min_max_sum_avg",
        """
        SELECT
            min(int_not_null),
            max(int_not_null),
            sum(int_nullable),
            avg(int_nullable)
        FROM {table}
        """,
        comparator="float_tolerant",
    )
    yield QueryCase(
        "float_sum_avg",
        """
        SELECT sum(float_val), avg(float_val), min(float_val), max(float_val)
        FROM {table}
        """,
        comparator="float_tolerant",
    )
    yield QueryCase(
        "group_by_segment_key",
        """
        SELECT
            group_key,
            count(*),
            count(int_nullable),
            sum(int_nullable),
            min(int_nullable),
            max(int_nullable)
        FROM {table}
        GROUP BY group_key
        ORDER BY group_key NULLS LAST
        """,
        comparator="float_tolerant",
    )
    yield QueryCase(
        "group_by_multiple_keys",
        """
        SELECT
            group_key,
            sub_key,
            count(*),
            sum(int_not_null),
            avg(float_val)
        FROM {table}
        GROUP BY group_key, sub_key
        ORDER BY group_key NULLS LAST, sub_key NULLS LAST
        """,
        comparator="float_tolerant",
    )
    yield QueryCase(
        "all_null_group_inputs",
        """
        SELECT
            group_key,
            count(all_null_input),
            sum(all_null_input),
            avg(all_null_input),
            min(all_null_input),
            max(all_null_input)
        FROM {table}
        GROUP BY group_key
        ORDER BY group_key NULLS LAST
        """,
        comparator="float_tolerant",
    )
    yield QueryCase(
        "having_on_aggregate",
        """
        SELECT group_key, count(*), sum(int_not_null)
        FROM {table}
        GROUP BY group_key
        HAVING count(*) >= 20 AND sum(int_not_null) <> 0
        ORDER BY group_key NULLS LAST
        """,
    )
    yield QueryCase(
        "where_segment_by_equality",
        """
        SELECT count(*), count(int_nullable), min(int_nullable), max(int_nullable), sum(int_nullable)
        FROM {table}
        WHERE group_key = 3
        """,
        comparator="float_tolerant",
    )
    yield QueryCase(
        "where_non_key_column",
        """
        SELECT group_key, count(*), sum(int_not_null), avg(float_val)
        FROM {table}
        WHERE filter_val BETWEEN -3 AND 4
        GROUP BY group_key
        ORDER BY group_key NULLS LAST
        """,
        comparator="float_tolerant",
    )
    yield QueryCase(
        "aligned_time_range",
        """
        SELECT count(*), sum(int_not_null), min(int_nullable), max(int_nullable)
        FROM {table}
        WHERE ts >= '2025-01-15 00:00:00+00'
          AND ts < '2025-01-18 00:00:00+00'
        """,
        comparator="float_tolerant",
    )
    yield QueryCase(
        "partial_time_range",
        """
        SELECT group_key, count(*), sum(int_not_null), avg(float_val)
        FROM {table}
        WHERE ts >= '2025-01-15 06:00:00+00'
          AND ts < '2025-01-16 18:00:00+00'
        GROUP BY group_key
        ORDER BY group_key NULLS LAST
        """,
        comparator="float_tolerant",
    )


def aggregate_extended_cases() -> Iterable[QueryCase]:
    yield QueryCase(
        "count_distinct_repeated_values",
        """
        SELECT
            count(DISTINCT repeat_val),
            count(DISTINCT group_key),
            count(DISTINCT int_nullable)
        FROM {table}
        """,
    )
    yield QueryCase(
        "grouped_count_distinct_repeated_values",
        """
        SELECT
            group_key,
            count(DISTINCT repeat_val),
            count(DISTINCT int_nullable)
        FROM {table}
        GROUP BY group_key
        ORDER BY group_key NULLS LAST
        """,
    )
    yield QueryCase(
        "aggregate_filter_clauses",
        """
        SELECT
            count(*) FILTER (WHERE filter_val > 0),
            count(int_nullable) FILTER (WHERE filter_val <= 0),
            sum(int_not_null) FILTER (WHERE filter_val BETWEEN -3 AND 3)
        FROM {table}
        """,
    )
    yield QueryCase(
        "aggregate_filter_clauses_null_sensitive",
        """
        SELECT
            count(*) FILTER (WHERE int_nullable IS NULL),
            sum(int_not_null) FILTER (WHERE group_key IS NULL),
            avg(float_val) FILTER (WHERE float_val IS NOT NULL AND int_nullable IS DISTINCT FROM 0),
            min(ts) FILTER (WHERE all_null_input IS NULL)
        FROM {table}
        """,
        comparator="float_tolerant",
    )
    yield QueryCase(
        "aggregate_over_expressions",
        """
        SELECT
            sum((int_not_null * 2)::bigint),
            avg(abs(float_val)),
            max(coalesce(int_nullable, -999)),
            min(date_trunc('hour', ts))
        FROM {table}
        """,
        comparator="float_tolerant",
    )
    yield QueryCase(
        "aggregate_empty_result_without_group_by",
        """
        SELECT count(*), count(int_nullable), sum(int_nullable), avg(float_val), min(ts), max(ts)
        FROM {table}
        WHERE ts >= '2030-01-01 00:00:00+00'
        """,
        comparator="float_tolerant",
    )
    yield QueryCase(
        "aggregate_empty_result_with_group_by",
        """
        SELECT group_key, count(*), sum(int_nullable)
        FROM {table}
        WHERE ts >= '2030-01-01 00:00:00+00'
        GROUP BY group_key
        ORDER BY group_key NULLS LAST
        """,
    )
    yield QueryCase(
        "grouping_sets_rollup",
        """
        SELECT group_key, sub_key, count(*), sum(int_not_null), grouping(group_key, sub_key) AS grouping_id
        FROM {table}
        GROUP BY ROLLUP (group_key, sub_key)
        ORDER BY grouping_id, group_key NULLS LAST, sub_key NULLS LAST
        """,
    )
    yield QueryCase(
        "ordered_array_aggregate_fallback",
        """
        SELECT group_key, array_agg(id ORDER BY ts DESC, id DESC)
        FROM {table}
        WHERE group_key IN (1, 2, 3)
        GROUP BY group_key
        ORDER BY group_key
        """,
    )
    yield QueryCase(
        "group_by_not_null_bucket",
        """
        SELECT
            bucket_not_null,
            count(*),
            sum(int_not_null),
            avg(float_val)
        FROM {table}
        GROUP BY bucket_not_null
        ORDER BY bucket_not_null
        """,
        comparator="float_tolerant",
    )
    yield QueryCase(
        "order_by_aggregate_limit",
        """
        SELECT group_key, count(*), sum(int_not_null)
        FROM {table}
        GROUP BY group_key
        ORDER BY sum(int_not_null) DESC NULLS LAST, count(*) DESC, group_key NULLS LAST
        LIMIT 5
        """,
    )
    yield QueryCase(
        "where_group_key_is_null",
        """
        SELECT count(*), count(int_nullable), sum(int_not_null), avg(float_val)
        FROM {table}
        WHERE group_key IS NULL
        """,
        comparator="float_tolerant",
    )
    yield QueryCase(
        "having_null_sensitive_aggregate",
        """
        SELECT group_key, count(*), count(all_null_input), sum(all_null_input)
        FROM {table}
        GROUP BY group_key
        HAVING sum(all_null_input) IS NULL
        ORDER BY group_key NULLS LAST
        """,
    )


def partition_segment_edge_cases() -> Iterable[QueryCase]:
    yield QueryCase(
        "exact_partition_start_inclusive",
        """
        SELECT id, ts, bucket, payload
        FROM {table}
        WHERE ts >= '2025-01-15 00:00:00+00'
          AND ts < '2025-01-16 00:00:00+00'
        ORDER BY ts, id
        """,
    )
    yield QueryCase(
        "partition_end_exclusive",
        """
        SELECT id, ts, bucket, payload
        FROM {table}
        WHERE ts >= '2025-01-14 00:00:00+00'
          AND ts < '2025-01-15 00:00:00+00'
        ORDER BY ts, id
        """,
    )
    yield QueryCase(
        "single_boundary_timestamp",
        """
        SELECT id, bucket, payload
        FROM {table}
        WHERE ts = '2025-01-16 00:00:00+00'
        ORDER BY id
        """,
    )
    yield QueryCase(
        "half_open_crosses_compressed_and_uncompressed",
        """
        SELECT id, ts, bucket, val, metric
        FROM {table}
        WHERE ts >= '2025-01-15 18:00:00+00'
          AND ts < '2025-01-17 02:00:00+00'
        ORDER BY ts, id
        """,
        comparator="float_tolerant",
    )
    yield QueryCase(
        "default_partition_only",
        """
        SELECT id, ts, bucket, payload
        FROM {table}
        WHERE ts < '2025-01-14 00:00:00+00'
           OR ts >= '2025-01-19 00:00:00+00'
        ORDER BY ts, id
        """,
    )
    yield QueryCase(
        "empty_partition_range",
        """
        SELECT id, ts, bucket
        FROM {table}
        WHERE ts >= '2025-01-18 00:00:00+00'
          AND ts < '2025-01-19 00:00:00+00'
        ORDER BY ts, id
        """,
    )
    yield QueryCase(
        "count_all_mixed_storage",
        """
        SELECT count(*)
        FROM {table}
        """,
    )
    yield QueryCase(
        "count_half_open_registered_partitions",
        """
        SELECT count(*), count(val), min(id), max(id)
        FROM {table}
        WHERE ts >= '2025-01-14 00:00:00+00'
          AND ts < '2025-01-18 00:00:00+00'
        """,
    )
    yield QueryCase(
        "grouped_aggregate_across_boundaries",
        """
        SELECT bucket, count(*), count(val), sum(val), avg(metric), min(ts), max(ts)
        FROM {table}
        WHERE ts >= '2025-01-14 00:00:00+00'
          AND ts < '2025-01-18 00:00:00+00'
        GROUP BY bucket
        ORDER BY bucket
        """,
        comparator="float_tolerant",
    )
    yield QueryCase(
        "segment_size_minus_one_projection",
        """
        SELECT id, device_id, val, payload
        FROM {table}
        WHERE bucket = 'compressed_4'
        ORDER BY ts, id
        """,
    )
    yield QueryCase(
        "segment_size_exact_projection",
        """
        SELECT id, device_id, val, payload
        FROM {table}
        WHERE bucket = 'compressed_5'
        ORDER BY ts, id
        """,
    )
    yield QueryCase(
        "segment_size_plus_one_projection",
        """
        SELECT id, device_id, val, payload
        FROM {table}
        WHERE bucket = 'uncompressed_6'
        ORDER BY ts, id
        """,
    )
    yield QueryCase(
        "two_segments_exact_projection",
        """
        SELECT id, device_id, val, payload
        FROM {table}
        WHERE bucket = 'compressed_10'
        ORDER BY ts, id
        """,
    )
    yield QueryCase(
        "topn_across_default_compressed_uncompressed",
        """
        SELECT id, ts, bucket, payload, val
        FROM {table}
        ORDER BY ts DESC, id DESC
        LIMIT 9
        """,
    )
    yield QueryCase(
        "filtered_topn_across_mixed_storage",
        """
        SELECT id, ts, device_id, bucket, payload
        FROM {table}
        WHERE device_id IS NULL OR val >= 35
        ORDER BY ts ASC, id ASC
        LIMIT 12
        """,
    )


def partition_segment_boundary_cases() -> Iterable[QueryCase]:
    yield QueryCase(
        "inclusive_upper_end_minus_epsilon",
        """
        SELECT id, ts, bucket, payload
        FROM {table}
        WHERE ts >= '2025-01-15 00:00:00+00'
          AND ts <= '2025-01-15 23:59:59.999999+00'
        ORDER BY ts, id
        """,
    )
    yield QueryCase(
        "strict_before_partition_start",
        """
        SELECT id, ts, bucket, payload
        FROM {table}
        WHERE ts < '2025-01-15 00:00:00+00'
        ORDER BY ts, id
        """,
    )
    yield QueryCase(
        "between_includes_next_partition_start",
        """
        SELECT id, ts, bucket, payload
        FROM {table}
        WHERE ts BETWEEN '2025-01-15 00:00:00+00'
                     AND '2025-01-16 00:00:00+00'
        ORDER BY ts, id
        """,
    )
    yield QueryCase(
        "default_old_plus_compressed_partition",
        """
        SELECT id, ts, bucket, payload
        FROM {table}
        WHERE ts < '2025-01-15 00:00:00+00'
        ORDER BY ts, id
        """,
    )
    yield QueryCase(
        "compressed_partition_plus_default_future",
        """
        SELECT id, ts, bucket, payload
        FROM {table}
        WHERE ts >= '2025-01-17 00:00:00+00'
        ORDER BY ts, id
        """,
    )
    yield QueryCase(
        "session_timezone_boundary_projection",
        """
        SELECT id, ts, bucket, payload
        FROM {table}
        WHERE ts >= '2025-01-15 00:00:00 Europe/Berlin'::timestamptz
          AND ts < '2025-01-16 00:00:00 Europe/Berlin'::timestamptz
        ORDER BY ts, id
        """,
    )


def partition_segment_direct_backfill_cases() -> Iterable[QueryCase]:
    yield QueryCase(
        "direct_backfill_registered_projection",
        """
        SELECT id, ts, device_id, bucket, val, metric, payload
        FROM {table}
        ORDER BY ts, id
        """,
        comparator="float_tolerant",
    )
    yield QueryCase(
        "direct_backfill_half_open_boundaries",
        """
        SELECT id, ts, bucket, val, metric
        FROM {table}
        WHERE ts >= '2025-01-15 00:00:00+00'
          AND ts < '2025-01-17 00:00:00+00'
        ORDER BY ts, id
        """,
        comparator="float_tolerant",
    )
    yield QueryCase(
        "direct_backfill_grouped_aggregate",
        """
        SELECT bucket, count(*), count(val), sum(val), avg(metric), min(ts), max(ts)
        FROM {table}
        GROUP BY bucket
        ORDER BY bucket
        """,
        comparator="float_tolerant",
    )
    yield QueryCase(
        "direct_backfill_topn",
        """
        SELECT id, ts, bucket, payload, val
        FROM {table}
        ORDER BY ts DESC, id DESC
        LIMIT 8
        """,
    )


def partition_segment_fastpath_cases() -> Iterable[QueryCase]:
    yield QueryCase(
        "count_all_mixed_storage",
        """
        SELECT count(*)
        FROM {table}
        """,
    )
    yield QueryCase(
        "count_half_open_registered_partitions",
        """
        SELECT count(*), count(val), min(id), max(id)
        FROM {table}
        WHERE ts >= '2025-01-14 00:00:00+00'
          AND ts < '2025-01-18 00:00:00+00'
        """,
    )
    yield QueryCase(
        "grouped_aggregate_across_boundaries",
        """
        SELECT bucket, count(*), count(val), sum(val), avg(metric), min(ts), max(ts)
        FROM {table}
        WHERE ts >= '2025-01-14 00:00:00+00'
          AND ts < '2025-01-18 00:00:00+00'
        GROUP BY bucket
        ORDER BY bucket
        """,
        comparator="float_tolerant",
    )


def codec_matrix_cases() -> Iterable[QueryCase]:
    yield QueryCase(
        "full_projection_ordered",
        """
        SELECT
            id, ts, device_id, dict_text, unique_text, active, small_int,
            int_val, large_int, repeated_int, float_val, observed_at,
            always_text, pattern_text, nullable_text
        FROM {table}
        ORDER BY ts, id
        """,
        comparator="float_tolerant",
    )
    for column in (
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
    ):
        yield QueryCase(
            f"single_column_projection_{column}",
            f"""
            SELECT id, {column}
            FROM {{table}}
            ORDER BY id
            """,
            comparator="float_tolerant" if column == "float_val" else "ordered_exact",
        )
    yield QueryCase(
        "reordered_projection_with_expressions",
        """
        SELECT
            coalesce(nullable_text, 'missing') AS nullable_text_filled,
            large_int::text AS large_int_text,
            small_int::bigint + int_val::bigint AS combined_ints,
            id,
            ts::date AS ts_date,
            length(unique_text) AS unique_text_len,
            pattern_text IS NULL AS pattern_text_is_null
        FROM {table}
        WHERE id < 20 OR id >= 1000
        ORDER BY id
        """,
    )
    yield QueryCase(
        "dictionary_text_equality",
        """
        SELECT id, dict_text, nullable_text
        FROM {table}
        WHERE dict_text = 'alpha'
        ORDER BY id
        """,
    )
    yield QueryCase(
        "dictionary_text_including_null",
        """
        SELECT id, dict_text
        FROM {table}
        WHERE dict_text IS NULL OR dict_text IN ('beta', 'delta')
        ORDER BY dict_text NULLS FIRST, id
        """,
    )
    yield QueryCase(
        "high_cardinality_text_equality",
        """
        SELECT id, unique_text
        FROM {table}
        WHERE unique_text IN ('token-0001-07919', 'token-0143-82417', 'token-0287-40053')
        ORDER BY id
        """,
    )
    yield QueryCase(
        "empty_string_vs_null_text",
        """
        SELECT id, unique_text, always_text, pattern_text, nullable_text
        FROM {table}
        WHERE unique_text = ''
           OR always_text = ''
           OR pattern_text = ''
           OR nullable_text = ''
           OR unique_text IS NULL
           OR pattern_text IS NULL
           OR nullable_text IS NULL
        ORDER BY id
        """,
    )
    yield QueryCase(
        "text_like_prefix_and_contains",
        """
        SELECT id, unique_text, nullable_text
        FROM {table}
        WHERE unique_text LIKE 'token-00%'
           OR nullable_text LIKE 'nullable-repeat-%'
        ORDER BY id
        """,
    )
    yield QueryCase(
        "text_like_escaped_literals",
        """
        SELECT id, unique_text, pattern_text
        FROM {table}
        WHERE unique_text LIKE '%100!%!_done' ESCAPE '!'
           OR pattern_text LIKE 'under!_score!_!%!_literal' ESCAPE '!'
           OR pattern_text LIKE 'literal!%underscore!_' ESCAPE '!'
        ORDER BY id
        """,
    )
    yield QueryCase(
        "quoted_and_long_text_projection",
        """
        SELECT id, unique_text, length(unique_text), nullable_text
        FROM {table}
        WHERE unique_text LIKE 'text with,%'
           OR unique_text LIKE 'long-%'
           OR nullable_text = 'contains,comma'
        ORDER BY id
        """,
    )
    yield QueryCase(
        "boolean_null_semantics",
        """
        SELECT id, active
        FROM {table}
        WHERE active IS NOT TRUE
        ORDER BY id
        """,
    )
    yield QueryCase(
        "small_integer_predicates",
        """
        SELECT id, small_int
        FROM {table}
        WHERE small_int IS NULL
           OR small_int BETWEEN -3 AND 3
           OR small_int IN (-30, 30)
        ORDER BY small_int NULLS FIRST, id
        """,
    )
    yield QueryCase(
        "integer_boundary_values",
        """
        SELECT id, small_int, int_val, large_int
        FROM {table}
        WHERE small_int IN (-32768, 32767)
           OR int_val IN (-2147483648, 2147483647)
           OR large_int IN (-9223372036854775807, 9223372036854775806)
        ORDER BY id
        """,
    )
    yield QueryCase(
        "integer_cast_and_coalesce_expressions",
        """
        SELECT
            id,
            coalesce(small_int::integer, -999999) AS small_int_filled,
            coalesce(int_val, -999999) AS int_val_filled,
            abs(repeated_int) AS repeated_abs
        FROM {table}
        WHERE coalesce(int_val, 0) BETWEEN -5 AND 5
           OR coalesce(small_int::integer, 0) IN (-32768, 32767, 0)
        ORDER BY id
        """,
    )
    yield QueryCase(
        "large_integer_predicates",
        """
        SELECT id, large_int
        FROM {table}
        WHERE large_int IS NULL
           OR large_int < -900000000
           OR large_int > 900000000
        ORDER BY large_int NULLS FIRST, id
        """,
    )
    yield QueryCase(
        "float_extreme_and_negative_zero_projection",
        """
        SELECT id, float_val, float_val = 0.0 AS equals_zero
        FROM {table}
        WHERE float_val IS NULL
           OR float_val = 0.0
           OR abs(float_val) >= 1000000000000.0
           OR abs(float_val) <= 0.000000000001
        ORDER BY id
        """,
        comparator="float_tolerant",
    )
    yield QueryCase(
        "repeated_value_constant_codec",
        """
        SELECT repeated_int, count(*), min(id), max(id)
        FROM {table}
        GROUP BY repeated_int
        ORDER BY repeated_int
        """,
    )
    yield QueryCase(
        "timestamp_predicates",
        """
        SELECT id, ts, observed_at
        FROM {table}
        WHERE ts >= '2025-01-15 01:00:00+00'
          AND ts < '2025-01-15 03:00:00+00'
          AND observed_at >= '2025-01-14 12:00:00+00'
        ORDER BY observed_at, id
        """,
    )
    yield QueryCase(
        "timestamp_expression_filters",
        """
        SELECT id, ts, observed_at, date_trunc('hour', observed_at) AS observed_hour
        FROM {table}
        WHERE observed_at::date = DATE '2025-01-14'
          AND extract(minute FROM observed_at)::integer IN (0, 7, 56, 59)
        ORDER BY observed_at, id
        """,
    )
    yield QueryCase(
        "group_by_compressed_text_and_bool",
        """
        SELECT dict_text, active, count(*), count(unique_text), count(float_val)
        FROM {table}
        GROUP BY dict_text, active
        ORDER BY dict_text NULLS LAST, active NULLS LAST
        """,
    )
    yield QueryCase(
        "group_by_null_pattern_columns",
        """
        SELECT
            pattern_text,
            nullable_text,
            count(*),
            count(pattern_text),
            count(nullable_text),
            min(id),
            max(id)
        FROM {table}
        WHERE id < 60 OR id >= 1000
        GROUP BY pattern_text, nullable_text
        ORDER BY pattern_text NULLS FIRST, nullable_text NULLS FIRST
        """,
    )
    yield QueryCase(
        "integer_and_float_aggregates",
        """
        SELECT
            count(*),
            count(small_int),
            min(small_int),
            max(small_int),
            min(int_val),
            max(int_val),
            sum(int_val::numeric),
            sum(large_int::numeric),
            round(avg(float_val)::numeric, 6),
            min(float_val),
            max(float_val)
        FROM {table}
        """,
        comparator="float_tolerant",
    )
    yield QueryCase(
        "distinct_text_and_counts",
        """
        SELECT
            count(DISTINCT dict_text),
            count(DISTINCT unique_text),
            count(DISTINCT always_text),
            count(DISTINCT pattern_text),
            count(DISTINCT nullable_text)
        FROM {table}
        """,
    )
    yield QueryCase(
        "distinct_values_ordered",
        """
        SELECT DISTINCT dict_text, active
        FROM {table}
        ORDER BY dict_text NULLS FIRST, active NULLS FIRST
        """,
    )
    yield QueryCase(
        "window_by_dictionary_text",
        """
        SELECT id, dict_text, large_int, rn
        FROM (
            SELECT
                id,
                dict_text,
                large_int,
                row_number() OVER (
                    PARTITION BY dict_text
                    ORDER BY large_int DESC NULLS LAST, id
                ) AS rn
            FROM {table}
            WHERE dict_text IS NOT NULL
        ) ranked
        WHERE rn <= 3
        ORDER BY dict_text, rn, id
        """,
    )
    yield QueryCase(
        "union_all_filtered_codec_scans",
        """
        SELECT id, source
        FROM (
            SELECT id, 'small_boundary' AS source
            FROM {table}
            WHERE small_int IN (-32768, 32767)
            UNION ALL
            SELECT id, 'quoted_text' AS source
            FROM {table}
            WHERE unique_text LIKE 'text with,%'
        ) unioned
        ORDER BY source, id
        """,
    )
    yield QueryCase(
        "except_removes_null_pattern_rows",
        """
        SELECT id
        FROM (
            SELECT id
            FROM {table}
            WHERE id < 40
            EXCEPT
            SELECT id
            FROM {table}
            WHERE pattern_text IS NULL
        ) remaining
        ORDER BY id
        """,
    )
    yield QueryCase(
        "intersect_text_and_numeric_filters",
        """
        SELECT id
        FROM (
            SELECT id
            FROM {table}
            WHERE unique_text LIKE 'token-01%'
            INTERSECT
            SELECT id
            FROM {table}
            WHERE int_val BETWEEN -20 AND 20
        ) matched
        ORDER BY id
        """,
    )
    yield QueryCase(
        "filtered_grouped_aggregates",
        """
        SELECT
            device_id,
            dict_text,
            count(*),
            sum(small_int),
            avg(float_val),
            min(observed_at),
            max(observed_at)
        FROM {table}
        WHERE active IS NOT FALSE
          AND (unique_text LIKE 'token-01%' OR nullable_text = 'same-value')
        GROUP BY device_id, dict_text
        ORDER BY device_id NULLS LAST, dict_text NULLS LAST
        """,
        comparator="float_tolerant",
    )
    yield QueryCase(
        "topn_across_codec_columns",
        """
        SELECT id, ts, dict_text, large_int, unique_text, nullable_text
        FROM {table}
        WHERE large_int IS NOT NULL AND dict_text IS NOT NULL
        ORDER BY large_int DESC, dict_text, id
        LIMIT 17
        """,
    )
    yield QueryCase(
        "numeric_window_over_codec_columns",
        """
        SELECT id, device_id, small_int, int_val, running_sum
        FROM (
            SELECT
                id,
                device_id,
                small_int,
                int_val,
                sum(coalesce(int_val, 0)) OVER (
                    PARTITION BY device_id
                    ORDER BY ts, id
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS running_sum
            FROM {table}
            WHERE device_id IS NOT NULL AND id < 80
        ) ranked
        ORDER BY device_id, id
        """,
    )


def rtabench_synthetic_cases() -> Iterable[QueryCase]:
    yield QueryCase(
        "fact_projection_ordered",
        """
        SELECT order_id, counter, event_created, event_type, satisfaction, processor, backup_processor
        FROM {table}
        WHERE event_created >= '2024-05-03 00:00:00+00'
          AND event_created < '2024-05-07 00:00:00+00'
        ORDER BY order_id, event_created, counter NULLS LAST
        """,
        comparator="float_tolerant",
    )
    yield QueryCase(
        "dimension_join_fact_inner",
        """
        SELECT oe.order_id, c.customer_id, c.country, oe.event_type, oe.event_created
        FROM orders o
        JOIN customers c ON c.customer_id = o.customer_id
        JOIN {table} oe ON oe.order_id = o.order_id
        WHERE c.country IN ('US', 'DE', 'FR')
          AND oe.event_type IN ('Delivered', 'Returned')
        ORDER BY c.country, oe.order_id, oe.event_created, oe.counter NULLS LAST
        """,
    )
    yield QueryCase(
        "fact_outer_side_dimension_join",
        """
        SELECT oe.order_id, oe.event_type, o.customer_id, c.state
        FROM {table} oe
        JOIN orders o ON o.order_id = oe.order_id
        JOIN customers c ON c.customer_id = o.customer_id
        WHERE oe.order_id BETWEEN 20 AND 45
          AND oe.processor IN ('proc-a', 'proc-c')
        ORDER BY oe.order_id, oe.event_created, oe.counter NULLS LAST
        """,
    )
    yield QueryCase(
        "fact_left_join_product_rollup",
        """
        SELECT
            oe.order_id,
            oe.event_type,
            count(oi.product_id) AS item_count,
            coalesce(sum(oi.amount * p.price), 0)::numeric(20,2) AS order_value
        FROM {table} oe
        LEFT JOIN order_items oi ON oi.order_id = oe.order_id
        LEFT JOIN products p ON p.product_id = oi.product_id
        WHERE oe.event_type IN ('Created', 'Delivered')
          AND oe.order_id <= 30
        GROUP BY oe.order_id, oe.event_created, oe.counter, oe.event_type
        ORDER BY oe.order_id, oe.event_created, oe.counter NULLS LAST
        """,
    )
    yield QueryCase(
        "exists_delivered_orders",
        """
        SELECT o.order_id, o.customer_id, o.created_at
        FROM orders o
        WHERE EXISTS (
            SELECT 1
            FROM {table} oe
            WHERE oe.order_id = o.order_id
              AND oe.event_type = 'Delivered'
        )
        ORDER BY o.order_id
        """,
    )
    yield QueryCase(
        "not_exists_cancelled_orders",
        """
        SELECT o.order_id, o.customer_id
        FROM orders o
        WHERE NOT EXISTS (
            SELECT 1
            FROM {table} oe
            WHERE oe.order_id = o.order_id
              AND oe.event_type = 'Cancelled'
        )
        ORDER BY o.order_id
        """,
    )
    yield QueryCase(
        "in_delivered_order_ids",
        """
        SELECT o.order_id, o.customer_id
        FROM orders o
        WHERE o.order_id IN (
            SELECT oe.order_id
            FROM {table} oe
            WHERE oe.event_type = 'Delivered'
              AND oe.event_created < '2024-05-10 00:00:00+00'
        )
        ORDER BY o.order_id
        """,
    )
    yield QueryCase(
        "not_in_returned_order_ids",
        """
        SELECT o.order_id, o.customer_id
        FROM orders o
        WHERE o.order_id NOT IN (
            SELECT oe.order_id
            FROM {table} oe
            WHERE oe.event_type = 'Returned'
        )
        ORDER BY o.order_id
        """,
    )
    yield QueryCase(
        "not_in_subquery_with_null_semantics",
        """
        SELECT o.order_id, o.customer_id
        FROM orders o
        WHERE o.order_id NOT IN (
            SELECT oe.counter
            FROM {table} oe
            WHERE oe.event_type = 'Returned'
               OR oe.counter IS NULL
        )
        ORDER BY o.order_id
        """,
    )
    yield QueryCase(
        "distinct_on_latest_per_order",
        """
        SELECT DISTINCT ON (oe.order_id)
            oe.order_id, oe.event_created, oe.event_type, oe.processor
        FROM {table} oe
        WHERE oe.order_id IN (5, 17, 42, 88, 123, 177)
        ORDER BY oe.order_id, oe.event_created DESC, oe.counter DESC NULLS LAST
        """,
    )
    yield QueryCase(
        "window_last_event_per_order",
        """
        SELECT order_id, event_created, event_type, processor
        FROM (
            SELECT
                oe.order_id,
                oe.event_created,
                oe.event_type,
                oe.processor,
                row_number() OVER (
                    PARTITION BY oe.order_id
                    ORDER BY oe.event_created DESC, oe.counter DESC NULLS LAST
                ) AS rn
            FROM {table} oe
            WHERE oe.order_id BETWEEN 40 AND 70
        ) ranked
        WHERE rn = 1
        ORDER BY order_id
        """,
    )
    yield QueryCase(
        "customer_revenue_from_delivered_events",
        """
        SELECT
            c.customer_id,
            c.country,
            sum(oi.amount * p.price)::numeric(20,2) AS revenue,
            count(DISTINCT o.order_id) AS orders
        FROM customers c
        JOIN orders o ON o.customer_id = c.customer_id
        JOIN order_items oi ON oi.order_id = o.order_id
        JOIN products p ON p.product_id = oi.product_id
        JOIN (
            SELECT DISTINCT order_id
            FROM {table}
            WHERE event_type = 'Delivered'
        ) delivered ON delivered.order_id = o.order_id
        WHERE o.created_at >= '2024-05-01 00:00:00+00'
          AND o.created_at < '2024-05-12 00:00:00+00'
        GROUP BY c.customer_id, c.country
        HAVING sum(oi.amount * p.price) > 1000
        ORDER BY revenue DESC, c.customer_id
        """,
    )
    yield QueryCase(
        "category_performance_join",
        """
        SELECT
            p.category,
            oe.event_type,
            count(*) AS event_item_rows,
            count(DISTINCT oe.order_id) AS orders,
            sum(oi.amount * p.price)::numeric(20,2) AS value
        FROM {table} oe
        JOIN order_items oi ON oi.order_id = oe.order_id
        JOIN products p ON p.product_id = oi.product_id
        WHERE oe.event_type IN ('Delivered', 'Returned')
          AND oe.event_created >= '2024-05-03 00:00:00+00'
          AND oe.event_created < '2024-05-13 00:00:00+00'
        GROUP BY p.category, oe.event_type
        ORDER BY p.category, oe.event_type
        """,
    )
    yield QueryCase(
        "semi_join_products_with_events",
        """
        SELECT p.product_id, p.category, p.price
        FROM products p
        WHERE EXISTS (
            SELECT 1
            FROM order_items oi
            JOIN {table} oe ON oe.order_id = oi.order_id
            WHERE oi.product_id = p.product_id
              AND oe.event_type = 'Delivered'
              AND oe.processor = 'proc-a'
        )
        ORDER BY p.category, p.product_id
        """,
    )
    yield QueryCase(
        "anti_join_products_without_returns",
        """
        SELECT p.product_id, p.category
        FROM products p
        WHERE NOT EXISTS (
            SELECT 1
            FROM order_items oi
            JOIN {table} oe ON oe.order_id = oi.order_id
            WHERE oi.product_id = p.product_id
              AND oe.event_type = 'Returned'
        )
        ORDER BY p.product_id
        """,
    )
    yield QueryCase(
        "event_transition_self_join",
        """
        SELECT
            first_ev.order_id,
            first_ev.event_type AS first_type,
            later_ev.event_type AS later_type,
            later_ev.event_created
        FROM {table} first_ev
        JOIN {table} later_ev
          ON later_ev.order_id = first_ev.order_id
         AND later_ev.event_created > first_ev.event_created
        WHERE first_ev.event_type = 'Created'
          AND later_ev.event_type IN ('Delivered', 'Returned')
          AND first_ev.order_id BETWEEN 1 AND 50
        ORDER BY first_ev.order_id, later_ev.event_created, later_ev.counter NULLS LAST
        """,
    )
    yield QueryCase(
        "lateral_latest_event_per_customer_order",
        """
        SELECT c.customer_id, o.order_id, latest.event_created, latest.event_type
        FROM customers c
        JOIN orders o ON o.customer_id = c.customer_id
        CROSS JOIN LATERAL (
            SELECT oe.event_created, oe.event_type
            FROM {table} oe
            WHERE oe.order_id = o.order_id
            ORDER BY oe.event_created DESC, oe.counter DESC NULLS LAST
            LIMIT 1
        ) latest
        WHERE c.customer_id IN (1, 7, 13, 19)
        ORDER BY c.customer_id, o.order_id
        """,
    )
    yield QueryCase(
        "left_join_lateral_allows_no_match",
        """
        SELECT c.customer_id, o.order_id, latest.event_created, latest.event_type
        FROM customers c
        JOIN orders o ON o.customer_id = c.customer_id
        LEFT JOIN LATERAL (
            SELECT oe.event_created, oe.event_type
            FROM {table} oe
            WHERE oe.order_id = o.order_id
              AND oe.event_type = 'Impossible Event'
            ORDER BY oe.event_created DESC, oe.counter DESC NULLS LAST
            LIMIT 1
        ) latest ON true
        WHERE c.customer_id IN (1, 7, 13, 19)
        ORDER BY c.customer_id, o.order_id
        """,
    )
    yield QueryCase(
        "processor_backup_coalesce_grouping",
        """
        SELECT
            processor,
            coalesce(backup_processor, 'none') AS backup,
            count(*) AS events,
            round(avg(satisfaction)::numeric, 4) AS avg_satisfaction
        FROM {table}
        WHERE event_created >= '2024-05-01 00:00:00+00'
          AND event_created < '2024-05-15 00:00:00+00'
        GROUP BY processor, coalesce(backup_processor, 'none')
        ORDER BY processor, backup
        """,
        comparator="float_tolerant",
    )
    yield QueryCase(
        "union_all_inner_outer_roles",
        """
        SELECT order_id, source
        FROM (
            SELECT oe.order_id, 'fact_filtered' AS source
            FROM {table} oe
            WHERE oe.order_id <= 8
              AND oe.event_type = 'Created'
            UNION ALL
            SELECT o.order_id, 'dimension_filtered' AS source
            FROM orders o
            JOIN {table} oe ON oe.order_id = o.order_id
            WHERE o.customer_id = 3
              AND oe.event_type = 'Delivered'
        ) unioned
        ORDER BY source, order_id
        """,
    )
    yield QueryCase(
        "right_join_fact_null_extension",
        """
        SELECT p.processor, oe.order_id, oe.event_type
        FROM {table} oe
        RIGHT JOIN processor_dim p ON p.processor = oe.processor
                              AND oe.event_type = 'Delivered'
                              AND oe.order_id BETWEEN 1 AND 60
        ORDER BY p.processor NULLS LAST, oe.order_id NULLS LAST, oe.event_created NULLS LAST
        """,
    )
    yield QueryCase(
        "full_outer_join_product_event_coverage",
        """
        SELECT
            coalesce(oi.product_id, -1) AS product_id,
            coalesce(oe.event_type, 'no-event') AS event_type,
            count(*) AS rows
        FROM (
            SELECT order_id, product_id
            FROM order_items
            WHERE product_id BETWEEN 1 AND 12
        ) oi
        FULL OUTER JOIN (
            SELECT order_id, event_type
            FROM {table}
            WHERE order_id BETWEEN 150 AND 168
              AND event_type IN ('Delivered', 'Returned')
        ) oe ON oe.order_id = oi.order_id
        GROUP BY coalesce(oi.product_id, -1), coalesce(oe.event_type, 'no-event')
        ORDER BY product_id, event_type
        """,
    )
    yield QueryCase(
        "nullable_backup_plain_equality_join",
        """
        SELECT oe.order_id, oe.event_type, oe.backup_processor, p.region
        FROM {table} oe
        JOIN processor_dim p ON p.processor = oe.backup_processor
        WHERE oe.order_id BETWEEN 12 AND 40
        ORDER BY oe.order_id, oe.event_created, oe.counter NULLS LAST
        """,
    )
    yield QueryCase(
        "nullable_backup_is_not_distinct_join",
        """
        SELECT oe.order_id, oe.event_type, oe.backup_processor, p.region
        FROM {table} oe
        JOIN processor_dim p
          ON p.processor IS NOT DISTINCT FROM oe.backup_processor
        WHERE oe.order_id BETWEEN 12 AND 40
        ORDER BY oe.order_id, oe.event_created, oe.counter NULLS LAST, p.region
        """,
    )
    yield QueryCase(
        "left_join_nullable_backup_dimension",
        """
        SELECT
            oe.order_id,
            oe.event_type,
            coalesce(p.region, 'unmatched') AS backup_region,
            count(*) AS events
        FROM {table} oe
        LEFT JOIN processor_dim p ON p.processor = oe.backup_processor
        WHERE oe.order_id BETWEEN 80 AND 98
        GROUP BY oe.order_id, oe.event_type, coalesce(p.region, 'unmatched')
        ORDER BY oe.order_id, oe.event_type, backup_region
        """,
    )
    yield QueryCase(
        "multi_column_latest_event_join",
        """
        SELECT oe.order_id, oe.event_created, oe.event_type, oe.processor
        FROM {table} oe
        JOIN (
            SELECT order_id, max(event_created) AS latest_created
            FROM {table}
            WHERE order_id BETWEEN 30 AND 55
            GROUP BY order_id
        ) latest
          ON latest.order_id = oe.order_id
         AND latest.latest_created = oe.event_created
        ORDER BY oe.order_id, oe.counter NULLS LAST
        """,
    )
    yield QueryCase(
        "multi_column_event_type_counter_join",
        """
        SELECT oe.order_id, oe.counter, oe.event_type, marker.marker
        FROM {table} oe
        JOIN (
            VALUES
                (10, 'Packed'::text),
                (11, 'Departed'::text),
                (12, 'Delivered'::text),
                (13, 'Returned'::text),
                (14, 'Cancelled'::text),
                (15, 'Created'::text)
        ) AS marker(order_id, marker)
          ON marker.order_id = oe.order_id
         AND marker.marker = oe.event_type
        ORDER BY oe.order_id, oe.event_created, oe.counter NULLS LAST
        """,
    )
    yield QueryCase(
        "using_join_against_duplicate_keys",
        """
        SELECT order_id, event_type, marker
        FROM (
            VALUES
                (10, 'Created'::text, 'first'::text),
                (10, 'Created'::text, 'duplicate'::text),
                (11, 'Packed'::text, 'second'::text),
                (12, 'Delivered'::text, 'third'::text)
        ) AS marker(order_id, event_type, marker)
        JOIN {table} USING (order_id, event_type)
        ORDER BY order_id, event_type, marker, event_created, counter NULLS LAST
        """,
    )
    yield QueryCase(
        "range_join_events_after_order_created",
        """
        SELECT o.order_id, count(*) AS events_after_created
        FROM orders o
        JOIN {table} oe
          ON oe.order_id = o.order_id
         AND oe.event_created >= o.created_at
         AND oe.event_created < o.created_at + interval '30 hours'
        WHERE o.order_id BETWEEN 45 AND 75
        GROUP BY o.order_id
        ORDER BY o.order_id
        """,
    )
    yield QueryCase(
        "bounded_self_range_join",
        """
        SELECT
            base.order_id,
            base.event_type AS base_type,
            follow.event_type AS follow_type,
            count(*) AS transitions
        FROM {table} base
        JOIN {table} follow
          ON follow.order_id = base.order_id
         AND follow.event_created > base.event_created
         AND follow.event_created <= base.event_created + interval '18 hours'
        WHERE base.order_id BETWEEN 60 AND 85
        GROUP BY base.order_id, base.event_type, follow.event_type
        ORDER BY base.order_id, base_type, follow_type
        """,
    )
    yield QueryCase(
        "fact_referenced_twice_with_dimension",
        """
        SELECT o.order_id, created.event_created AS created_at, delivered.event_created AS delivered_at, c.country
        FROM orders o
        JOIN customers c ON c.customer_id = o.customer_id
        JOIN {table} created
          ON created.order_id = o.order_id
         AND created.event_type = 'Created'
        LEFT JOIN {table} delivered
          ON delivered.order_id = o.order_id
         AND delivered.event_type = 'Delivered'
        WHERE o.order_id BETWEEN 1 AND 50
        ORDER BY o.order_id, delivered.event_created NULLS LAST
        """,
    )
    yield QueryCase(
        "except_joined_returned_orders",
        """
        SELECT order_id
        FROM (
            SELECT o.order_id
            FROM orders o
            JOIN {table} oe ON oe.order_id = o.order_id
            WHERE oe.event_type = 'Delivered'
            EXCEPT
            SELECT o.order_id
            FROM orders o
            JOIN {table} oe ON oe.order_id = o.order_id
            WHERE oe.event_type = 'Returned'
        ) remaining
        ORDER BY order_id
        """,
    )
    yield QueryCase(
        "intersect_joined_processor_orders",
        """
        SELECT order_id
        FROM (
            SELECT o.order_id
            FROM orders o
            JOIN {table} oe ON oe.order_id = o.order_id
            WHERE oe.processor = 'proc-a'
            INTERSECT
            SELECT o.order_id
            FROM orders o
            JOIN {table} oe ON oe.order_id = o.order_id
            WHERE oe.event_type = 'Delivered'
        ) matched
        ORDER BY order_id
        """,
    )
    yield QueryCase(
        "correlated_event_count_per_order",
        """
        SELECT
            o.order_id,
            o.customer_id,
            (
                SELECT count(*)
                FROM {table} oe
                WHERE oe.order_id = o.order_id
                  AND oe.event_type <> 'Created'
            ) AS non_created_events
        FROM orders o
        WHERE o.order_id BETWEEN 1 AND 35
        ORDER BY o.order_id
        """,
    )
    yield QueryCase(
        "correlated_max_event_timestamp",
        """
        SELECT
            o.order_id,
            (
                SELECT max(oe.event_created)
                FROM {table} oe
                WHERE oe.order_id = o.order_id
            ) AS max_event_created
        FROM orders o
        WHERE o.customer_id IN (2, 8, 14, 20)
        ORDER BY o.order_id
        """,
    )
    yield QueryCase(
        "correlated_avg_satisfaction_by_customer",
        """
        SELECT
            c.customer_id,
            round((
                SELECT avg(oe.satisfaction)::numeric
                FROM orders o
                JOIN {table} oe ON oe.order_id = o.order_id
                WHERE o.customer_id = c.customer_id
            ), 4) AS avg_satisfaction
        FROM customers c
        WHERE c.customer_id BETWEEN 1 AND 12
        ORDER BY c.customer_id
        """,
        comparator="float_tolerant",
    )
    yield QueryCase(
        "rtabench_q0004_customer_order_events",
        """
        SELECT
            c.country,
            date_trunc('day', oe.event_created) AS day,
            count(DISTINCT o.order_id) AS orders,
            count(*) AS events
        FROM customers c
        JOIN orders o ON o.customer_id = c.customer_id
        JOIN {table} oe ON oe.order_id = o.order_id
        WHERE oe.event_created >= '2024-05-04 00:00:00+00'
          AND oe.event_created < '2024-05-11 00:00:00+00'
        GROUP BY c.country, date_trunc('day', oe.event_created)
        ORDER BY c.country, day
        """,
    )
    yield QueryCase(
        "rtabench_q0012_product_category_funnel",
        """
        SELECT
            p.category,
            count(*) FILTER (WHERE oe.event_type = 'Created') AS created_events,
            count(*) FILTER (WHERE oe.event_type = 'Delivered') AS delivered_events,
            count(*) FILTER (WHERE oe.event_type = 'Returned') AS returned_events
        FROM {table} oe
        JOIN order_items oi ON oi.order_id = oe.order_id
        JOIN products p ON p.product_id = oi.product_id
        WHERE oe.event_created >= '2024-05-01 00:00:00+00'
          AND oe.event_created < '2024-05-15 00:00:00+00'
        GROUP BY p.category
        ORDER BY p.category
        """,
    )
    yield QueryCase(
        "rtabench_q0017_processor_latency_rollup",
        """
        SELECT
            first_ev.processor,
            first_ev.event_type AS from_type,
            later_ev.event_type AS to_type,
            count(*) AS transitions,
            round(avg(extract(epoch FROM later_ev.event_created - first_ev.event_created))::numeric, 2)
                AS avg_seconds
        FROM {table} first_ev
        JOIN {table} later_ev
          ON later_ev.order_id = first_ev.order_id
         AND later_ev.event_created > first_ev.event_created
        WHERE first_ev.event_type IN ('Created', 'Packed')
          AND later_ev.event_type IN ('Delivered', 'Returned', 'Cancelled')
        GROUP BY first_ev.processor, first_ev.event_type, later_ev.event_type
        ORDER BY first_ev.processor, from_type, to_type
        """,
        comparator="float_tolerant",
    )


def partition_segment_plan_shape_cases() -> Iterable[QueryCase]:
    yield QueryCase(
        "or_ranges_across_pruning_boundaries",
        """
        SELECT id, ts, bucket, payload
        FROM {table}
        WHERE (
            ts >= '2025-01-14 00:00:00+00'
            AND ts < '2025-01-15 00:00:00+00'
        )
        OR (
            ts >= '2025-01-17 00:00:00+00'
            AND ts < '2025-01-18 00:00:00+00'
        )
        ORDER BY ts, id
        """,
    )
    yield QueryCase(
        "negated_time_range",
        """
        SELECT id, ts, bucket, payload
        FROM {table}
        WHERE NOT (
            ts >= '2025-01-15 00:00:00+00'
            AND ts < '2025-01-16 00:00:00+00'
        )
        ORDER BY ts, id
        """,
    )
    yield QueryCase(
        "date_cast_time_filter",
        """
        SELECT id, ts, bucket, payload
        FROM {table}
        WHERE ts::date = DATE '2025-01-15'
        ORDER BY ts, id
        """,
    )
    yield QueryCase(
        "date_trunc_time_filter",
        """
        SELECT id, ts, bucket, payload
        FROM {table}
        WHERE date_trunc('day', ts) = '2025-01-15 00:00:00+00'::timestamptz
        ORDER BY ts, id
        """,
    )
    yield QueryCase(
        "distinct_bucket_across_boundaries",
        """
        SELECT DISTINCT bucket
        FROM {table}
        WHERE ts >= '2025-01-14 00:00:00+00'
          AND ts < '2025-01-19 00:00:00+00'
        ORDER BY bucket
        """,
    )
    yield QueryCase(
        "distinct_on_latest_per_bucket",
        """
        SELECT DISTINCT ON (bucket) bucket, id, ts, payload
        FROM {table}
        WHERE ts >= '2025-01-14 00:00:00+00'
          AND ts < '2025-01-19 00:00:00+00'
        ORDER BY bucket, ts DESC, id DESC
        """,
    )
    yield QueryCase(
        "window_row_number_by_bucket",
        """
        SELECT
            id,
            bucket,
            row_number() OVER (PARTITION BY bucket ORDER BY ts, id) AS bucket_rownum,
            count(*) OVER (PARTITION BY bucket) AS bucket_count
        FROM {table}
        WHERE ts >= '2025-01-14 00:00:00+00'
          AND ts < '2025-01-18 00:00:00+00'
        ORDER BY bucket, bucket_rownum, id
        """,
    )
    yield QueryCase(
        "union_all_default_and_registered",
        """
        SELECT id, bucket, source
        FROM (
            SELECT id, bucket, 'default_old' AS source
            FROM {table}
            WHERE ts < '2025-01-14 00:00:00+00'
            UNION ALL
            SELECT id, bucket, 'registered' AS source
            FROM {table}
            WHERE ts >= '2025-01-17 00:00:00+00'
              AND ts < '2025-01-18 00:00:00+00'
        ) AS unioned
        ORDER BY source, id
        """,
    )
    yield QueryCase(
        "intersect_registered_ids",
        """
        SELECT id
        FROM (
            SELECT id
            FROM {table}
            WHERE ts >= '2025-01-14 00:00:00+00'
              AND ts < '2025-01-18 00:00:00+00'
            INTERSECT
            SELECT id
            FROM {table}
            WHERE val IS NULL OR val >= 30
        ) AS intersected
        ORDER BY id
        """,
    )
    yield QueryCase(
        "except_removes_uncompressed_partition",
        """
        SELECT id
        FROM (
            SELECT id
            FROM {table}
            WHERE ts >= '2025-01-14 00:00:00+00'
              AND ts < '2025-01-18 00:00:00+00'
            EXCEPT
            SELECT id
            FROM {table}
            WHERE ts >= '2025-01-16 00:00:00+00'
              AND ts < '2025-01-17 00:00:00+00'
        ) AS remaining
        ORDER BY id
        """,
    )
    yield QueryCase(
        "semi_join_exists_bucket_peer",
        """
        SELECT t.id, t.ts, t.bucket, t.payload
        FROM {table} t
        WHERE t.ts >= '2025-01-14 00:00:00+00'
          AND t.ts < '2025-01-18 00:00:00+00'
          AND EXISTS (
              SELECT 1
              FROM {table} peer
              WHERE peer.bucket = t.bucket
                AND peer.id <> t.id
                AND peer.val IS NULL
          )
        ORDER BY t.ts, t.id
        """,
    )
    yield QueryCase(
        "anti_join_not_exists_later_bucket_row",
        """
        SELECT t.id, t.ts, t.bucket, t.payload
        FROM {table} t
        WHERE t.ts >= '2025-01-14 00:00:00+00'
          AND t.ts < '2025-01-18 00:00:00+00'
          AND NOT EXISTS (
              SELECT 1
              FROM {table} later
              WHERE later.bucket = t.bucket
                AND later.ts > t.ts
                AND later.ts < '2025-01-18 00:00:00+00'
          )
        ORDER BY t.ts, t.id
        """,
    )
    yield QueryCase(
        "lateral_top1_per_bucket",
        """
        SELECT buckets.bucket, picked.id, picked.ts, picked.payload
        FROM (
            SELECT DISTINCT bucket
            FROM {table}
            WHERE ts >= '2025-01-14 00:00:00+00'
              AND ts < '2025-01-18 00:00:00+00'
        ) AS buckets
        CROSS JOIN LATERAL (
            SELECT id, ts, payload
            FROM {table} t
            WHERE t.bucket = buckets.bucket
              AND t.ts >= '2025-01-14 00:00:00+00'
              AND t.ts < '2025-01-18 00:00:00+00'
            ORDER BY ts DESC, id DESC
            LIMIT 1
        ) AS picked
        ORDER BY buckets.bucket
        """,
    )
    yield QueryCase(
        "full_ordered_scan_across_all_storage",
        """
        SELECT id, ts, bucket, device_id, val, metric, payload
        FROM {table}
        ORDER BY ts, id
        """,
        comparator="float_tolerant",
    )
    yield QueryCase(
        "empty_partition_aggregate_semantics",
        """
        SELECT count(*), count(val), sum(val), avg(metric), min(ts), max(ts)
        FROM {table}
        WHERE ts >= '2025-01-18 00:00:00+00'
          AND ts < '2025-01-19 00:00:00+00'
        """,
        comparator="float_tolerant",
    )
