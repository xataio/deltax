"""Predicate-matrix correctness coverage."""

import pytest

from .datasets import create_predicate_matrix_pair
from .harness import assert_query_case
from .querygen import predicate_matrix_cases


PREDICATE_LAYOUTS = (
    ("time_ordered", ("ts", "id")),
    ("value_ordered", ("int_val", "ts", "id")),
)


@pytest.fixture(params=PREDICATE_LAYOUTS, ids=lambda layout: layout[0])
def predicate_events(db, request):
    layout_name, order_by = request.param
    return create_predicate_matrix_pair(
        db,
        deltax_table=f"predicate_events_{layout_name}",
        order_by=order_by,
    )


@pytest.mark.parametrize("case", list(predicate_matrix_cases()), ids=lambda case: case.name)
def test_predicate_matrix_matches_plain_postgres(predicate_events, db, case):
    plain_table, deltax_table = predicate_events
    assert_query_case(
        db,
        case,
        plain_table=plain_table,
        deltax_table=deltax_table,
    )
