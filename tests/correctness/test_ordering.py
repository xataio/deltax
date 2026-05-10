"""Ordering and Top-N correctness coverage."""

import pytest

from .datasets import create_ordering_edges_pair
from .harness import assert_query_case
from .querygen import ordering_topn_cases


ORDERING_LAYOUTS = (
    ("ts_ordered", ("ts",)),
    ("value_ordered", ("sort_val", "ts")),
)


@pytest.fixture(params=ORDERING_LAYOUTS, ids=lambda layout: layout[0])
def ordering_edges(db, request):
    layout_name, order_by = request.param
    return create_ordering_edges_pair(
        db,
        deltax_table=f"ordering_edges_{layout_name}",
        order_by=order_by,
    )


@pytest.mark.parametrize("case", list(ordering_topn_cases()), ids=lambda case: case.name)
def test_ordering_topn_matches_plain_postgres(ordering_edges, db, case):
    plain_table, deltax_table = ordering_edges
    assert_query_case(
        db,
        case,
        plain_table=plain_table,
        deltax_table=deltax_table,
    )
