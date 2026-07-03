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
from .harness import QueryCase, assert_query_case
from .querygen import collation_topn_cases


pytestmark = pytest.mark.smoke


# Byte-order collations (C, POSIX) plus the database default. The correctness
# container's default collation is linguistic (en_US.utf8), so a C/POSIX column
# sorts correctly ONLY if change 3's byte-order fast path engages — otherwise
# deltax would fall back to the default collation and diverge. That makes these
# layouts a real test of the fast path, not a tautology.
COLLATION_LAYOUTS = (
    ("c", "C"),
    ("posix", "POSIX"),
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


@pytest.mark.xfail(
    strict=True,
    reason="Pre-existing (not change 3): the Top-N text path sorts via "
    "cmp_nullable_str_collation, which hardcodes DEFAULT_COLLATION_OID instead "
    "of the sort column's own collation. An ICU-collated column is therefore "
    "ordered by the database default collation, giving wrong Top-N rows. "
    "collation_is_byte_order only rescues C/POSIX/C.UTF-8 columns. When the "
    "underlying comparison is fixed to honour the column collation, this test "
    "will XPASS and should be promoted into COLLATION_LAYOUTS.",
)
def test_icu_column_collation_topn_matches_plain_postgres(db):
    plain_table, deltax_table = create_collation_edges_pair(
        db,
        deltax_table="collation_edges_icu_unicode",
        sort_collation="unicode",
    )
    # Ascending top-N lands in the leading space/punctuation/digit cluster,
    # where ICU (ignore-punctuation) and en_US.utf8 disagree deterministically.
    assert_query_case(
        db,
        QueryCase(
            "icu_sort_text_asc",
            "SELECT id, sort_text FROM {table} ORDER BY sort_text ASC LIMIT 20",
        ),
        plain_table=plain_table,
        deltax_table=deltax_table,
    )
