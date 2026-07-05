"""Collation correctness for the Top-N text byte-order fast path.

Change 3 of the bloom-improvements work lets the parallel Top-N text path
compare by raw UTF-8 bytes (skipping strcoll and pruning workers to the exact
LIMIT) when the sort column's collation is byte-order equivalent. A false
positive would silently return byte-sorted rows under a linguistic collation.

These are differential tests: the same query runs against a plain-PostgreSQL
table and a pg_deltax table sharing the identical schema/collation, and the
row sequences must match exactly. Each collation is a separate parametrized
layout so the byte-order (C, POSIX), linguistic (ICU ``unicode``), and
database-default cases are all covered without needing multiple database
encodings — the per-column ``COLLATE`` clause is what the fast path inspects.
"""

import pytest

from .datasets import create_collation_edges_pair
from .harness import assert_query_case
from .querygen import collation_topn_cases


pytestmark = pytest.mark.smoke


# Byte-order collations (C, POSIX), a linguistic ICU collation (unicode), and
# the database default. The correctness container's default collation is
# linguistic (en_US.utf8), so a C/POSIX column sorts correctly ONLY if the
# byte-order fast path engages, and the ICU column sorts correctly ONLY if the
# Top-N comparison honours the column's own collation (varstr_cmp) rather than
# the database default — so every layout is a real test, not a tautology.
COLLATION_LAYOUTS = (
    ("c", "C"),
    ("posix", "POSIX"),
    ("icu_unicode", "unicode"),
    ("db_default", None),
)


@pytest.fixture(params=COLLATION_LAYOUTS, ids=lambda layout: layout[0])
def collation_edges(db, request):
    layout_name, collation = request.param
    return create_collation_edges_pair(
        db,
        deltax_table=f"collation_edges_{layout_name}",
        sort_collation=collation,
    )


@pytest.mark.parametrize("case", list(collation_topn_cases()), ids=lambda case: case.name)
def test_collation_topn_matches_plain_postgres(collation_edges, db, case):
    plain_table, deltax_table = collation_edges
    assert_query_case(
        db,
        case,
        plain_table=plain_table,
        deltax_table=deltax_table,
    )
