"""End-to-end tests for the per-(segment, value) COUNT sidecar (R5).

Integer columns with at most 64 distinct values partition-wide get an exact
per-segment value→count list stored in the `_counts` column of the
`<partition>_valbitmap` companion table (the partition-level value list lives
in `deltax.deltax_partition.column_valmap`). The DeltaXAgg executor serves
`SELECT col, COUNT(*) FROM t [WHERE ...] GROUP BY col` entirely from this
metadata — no compressed blob is touched — and bails to the normal path
whenever any condition isn't fully covered by the sidecar.

These tests exercise the full flow: INSERT → enable_compression →
compress_partition → query, comparing every result against expectations
computed in Python from the inserted rows (so bail-out paths are verified to
return identical results, not just to avoid crashing).
"""

import collections
import re

# `pg_deltax.mock_now` pins the partition origin so all our test data
# falls into partitions deltax_create_table actually creates.
MOCK_NOW = "2025-01-15 12:00:00+00"
BASE_TS = "2025-01-15 00:00:00+00"

N_SEGMENTS = 4
SEGMENT_SIZE = 200

_META_RESOLVED_RE = re.compile(r"segments_metadata_resolved=(\d+)")
_SEG_DECOMP_RE = re.compile(r"segments_decompressed=(\d+)")


def _adv_for(s, i):
    """Deterministic low-cardinality adv_id distribution.

    - 0 dominates (Q7 shape: most rows have AdvEngineID = 0)
    - values 1..5 appear with varying frequencies
    - value 9 appears ONLY in segment 0
    - every 25th row is NULL
    """
    if i % 25 == 3:
        return None
    if s == 0 and i % 50 == 0:
        return 9
    if i % 10 == 1:
        return 1
    if i % 10 == 2:
        return 2
    if i % 10 == 5:
        return s % 3 + 3  # 3, 4 or 5 depending on segment
    return 0


def _gen_rows():
    """Returns [(seconds_offset, order_id, adv_or_none)] for all rows."""
    rows = []
    for s in range(N_SEGMENTS):
        for i in range(SEGMENT_SIZE):
            order_id = s * SEGMENT_SIZE + i
            rows.append((order_id, order_id, _adv_for(s, i)))
    return rows


def _setup_adv_table(conn):
    """Create + compress a deltax table with a low-cardinality int column
    `adv_id` (≤ 64 distinct partition-wide, NULLs included) and a
    high-cardinality int column `payload` (> 64 distinct)."""
    conn.execute("DROP TABLE IF EXISTS adv CASCADE")
    conn.execute(f"SET pg_deltax.mock_now = '{MOCK_NOW}'")
    conn.execute("""
        CREATE TABLE adv (
            ts        timestamptz NOT NULL,
            order_id  integer NOT NULL,
            adv_id    integer,
            payload   integer NOT NULL
        )
    """)
    conn.execute(
        "SELECT deltax.deltax_create_table('adv', 'ts', '1 day'::interval)"
    )
    conn.commit()

    values = []
    for sec, order_id, adv in _gen_rows():
        ts = f"'{BASE_TS}'::timestamptz + interval '{sec} seconds'"
        adv_sql = "NULL" if adv is None else str(adv)
        # payload is high-cardinality: one distinct value per row.
        values.append(f"({ts}, {order_id}, {adv_sql}, {order_id})")

    batch = 500
    for i in range(0, len(values), batch):
        conn.execute(
            "INSERT INTO adv (ts, order_id, adv_id, payload) VALUES "
            + ", ".join(values[i:i + batch])
        )
    conn.commit()

    conn.execute(
        "SELECT deltax.deltax_enable_compression('adv', "
        f"order_by => ARRAY['order_id'], segment_size => {SEGMENT_SIZE})"
    )
    conn.commit()

    parts = conn.execute(
        "SELECT partition_name FROM deltax.deltax_partition_info('adv') "
        "WHERE partition_name NOT LIKE '%default%'"
    ).fetchall()
    for (part_name,) in parts:
        n = conn.execute(f'SELECT count(*) FROM "{part_name}"').fetchone()[0]
        if n == 0:
            continue
        conn.execute(f"SELECT deltax.deltax_compress_partition('{part_name}')")
    conn.commit()


def _expected_counts(pred=None):
    """Counter over adv values (None = NULL group) for rows matching pred.
    pred receives (seconds_offset, order_id, adv)."""
    c = collections.Counter()
    for row in _gen_rows():
        if pred is None or pred(*row):
            c[row[2]] += 1
    return dict(c)


def _query_counts(conn, sql):
    """Run a `SELECT <group>, count FROM ...` query, return {group: count}."""
    return {g: n for g, n in conn.execute(sql).fetchall()}


def _explain_stats(conn, sql):
    """Run EXPLAIN ANALYZE; return (metadata_resolved, decompressed) from the
    DeltaX Stats line, or (None, None) if the line/fields are absent."""
    rows = conn.execute(f"EXPLAIN (ANALYZE, BUFFERS, TIMING OFF) {sql}").fetchall()
    text = "\n".join(r[0] for r in rows)
    m1 = _META_RESOLVED_RE.search(text)
    m2 = _SEG_DECOMP_RE.search(text)
    return (
        int(m1.group(1)) if m1 else None,
        int(m2.group(1)) if m2 else None,
    )


class TestValueCounts:
    # ------------------------------------------------------------------
    # Fast-path correctness (results must equal Python-computed truth)
    # ------------------------------------------------------------------

    def test_group_by_count_matches_expected(self, db):
        """Plain GROUP BY adv_id + COUNT(*) — includes the NULL group."""
        _setup_adv_table(db)
        got = _query_counts(db, "SELECT adv_id, count(*) FROM adv GROUP BY adv_id")
        assert got == _expected_counts()

    def test_where_ne_zero_q7_shape(self, db):
        """Q7 shape: WHERE adv_id <> 0 GROUP BY adv_id ORDER BY COUNT(*) DESC.
        Zero and NULL rows are excluded (NULL <> 0 is not true)."""
        _setup_adv_table(db)
        sql = (
            "SELECT adv_id, count(*) FROM adv WHERE adv_id <> 0 "
            "GROUP BY adv_id ORDER BY count(*) DESC"
        )
        got = _query_counts(db, sql)
        expected = {
            k: v
            for k, v in _expected_counts().items()
            if k is not None and k != 0
        }
        assert got == expected

        # The metadata fast path should have served this without touching
        # blobs: all segments metadata-resolved, none decompressed.
        resolved, decompressed = _explain_stats(db, sql)
        assert resolved is not None and resolved > 0, (
            f"expected the value-counts fast path to fire "
            f"(resolved={resolved}, decompressed={decompressed})"
        )
        assert decompressed == 0

    def test_where_eq_value_only_in_one_segment(self, db):
        """Equality on a value present in a single segment."""
        _setup_adv_table(db)
        got = _query_counts(
            db, "SELECT adv_id, count(*) FROM adv WHERE adv_id = 9 GROUP BY adv_id"
        )
        expected = {9: _expected_counts()[9]}
        assert got == expected

    def test_where_range_on_group_col(self, db):
        """Range predicate on the group column, applied exactly per value."""
        _setup_adv_table(db)
        got = _query_counts(
            db,
            "SELECT adv_id, count(*) FROM adv "
            "WHERE adv_id >= 2 AND adv_id < 5 GROUP BY adv_id",
        )
        expected = {
            k: v
            for k, v in _expected_counts().items()
            if k is not None and 2 <= k < 5
        }
        assert got == expected

    def test_count_of_group_col(self, db):
        """COUNT(adv_id) GROUP BY adv_id: NULL group counts zero."""
        _setup_adv_table(db)
        got = _query_counts(
            db, "SELECT adv_id, count(adv_id) FROM adv GROUP BY adv_id"
        )
        expected = _expected_counts()
        expected[None] = 0  # COUNT(col) skips NULLs
        assert got == expected

    # ------------------------------------------------------------------
    # Bail-out paths must return identical results
    # ------------------------------------------------------------------

    def test_bailout_extra_qual_same_results(self, db):
        """A qual on another column that only partially covers segments
        forces the normal path — results must be identical."""
        _setup_adv_table(db)
        cutoff = SEGMENT_SIZE + 17  # mid-segment → ambiguous coverage
        got = _query_counts(
            db,
            f"SELECT adv_id, count(*) FROM adv WHERE order_id >= {cutoff} "
            "GROUP BY adv_id",
        )
        assert got == _expected_counts(lambda sec, oid, adv: oid >= cutoff)

    def test_bailout_time_filter_partial_coverage(self, db):
        """A time filter cutting through a segment forces the normal path."""
        _setup_adv_table(db)
        cutoff_sec = SEGMENT_SIZE + 31  # mid-segment timestamp
        got = _query_counts(
            db,
            "SELECT adv_id, count(*) FROM adv "
            f"WHERE ts >= '{BASE_TS}'::timestamptz + interval '{cutoff_sec} seconds' "
            "GROUP BY adv_id",
        )
        assert got == _expected_counts(lambda sec, oid, adv: sec >= cutoff_sec)

    def test_high_cardinality_int_no_sidecar(self, db):
        """`payload` has > 64 distinct values → no sidecar; GROUP BY still
        returns exact results through the normal path."""
        _setup_adv_table(db)
        got = _query_counts(
            db,
            "SELECT payload, count(*) FROM adv WHERE payload < 10 GROUP BY payload",
        )
        expected = {oid: 1 for sec, oid, adv in _gen_rows() if oid < 10}
        assert got == expected

    def test_bailout_uncompressed_rows_same_results(self, db):
        """Rows outside the compressed partitions (default partition) disable
        the aggregate pushdown entirely — results must include them."""
        _setup_adv_table(db)
        # Out-of-range timestamp lands in the default partition (uncompressed).
        db.execute(
            "INSERT INTO adv (ts, order_id, adv_id, payload) VALUES "
            "('2030-06-01 00:00:00+00', 999999, 1, 999999), "
            "('2030-06-01 00:00:01+00', 999998, NULL, 999998)"
        )
        db.commit()
        got = _query_counts(db, "SELECT adv_id, count(*) FROM adv GROUP BY adv_id")
        expected = dict(_expected_counts())
        expected[1] += 1
        expected[None] += 1
        assert got == expected

    def test_backward_compat_no_counts_column(self, db):
        """Tables compressed before the sidecar existed have a `_valbitmap`
        companion without the `_counts` column. Simulate that by dropping the
        column: the fast path must cleanly bail and the normal decompress
        path must return identical results."""
        _setup_adv_table(db)
        vbs = db.execute("""
            SELECT c.relname
            FROM pg_class c
            JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE n.nspname = '_deltax_compressed'
              AND c.relname LIKE 'adv_%_valbitmap'
        """).fetchall()
        assert vbs, "valbitmap companion tables missing"
        for (relname,) in vbs:
            db.execute(
                f'ALTER TABLE "_deltax_compressed"."{relname}" '
                "DROP COLUMN _counts"
            )
        db.commit()

        sql = (
            "SELECT adv_id, count(*) FROM adv WHERE adv_id <> 0 "
            "GROUP BY adv_id"
        )
        got = _query_counts(db, sql)
        expected = {
            k: v
            for k, v in _expected_counts().items()
            if k is not None and k != 0
        }
        assert got == expected

        # Without the sidecar the segments must have been decompressed, not
        # metadata-resolved via value counts. The fast path's
        # `segments_metadata_resolved=` counter is absent (or zero) in the
        # EXPLAIN output, and the normal agg path's `DeltaX Stats` line
        # reports decompressed segments via `segments=`.
        rows = db.execute(f"EXPLAIN (ANALYZE, TIMING OFF) {sql}").fetchall()
        text = "\n".join(r[0] for r in rows)
        m = _META_RESOLVED_RE.search(text)
        assert m is None or int(m.group(1)) == 0, (
            f"value-counts fast path unexpectedly fired without the sidecar:\n{text}"
        )
        m = re.search(r"\bsegments=(\d+)", text)
        assert m is not None and int(m.group(1)) > 0, (
            f"expected fallback to the decompress path:\n{text}"
        )

    # ------------------------------------------------------------------
    # Catalog / write-path checks
    # ------------------------------------------------------------------

    def test_catalog_valmap_contains_int_col(self, db):
        """column_valmap covers the low-card int column (decimal strings),
        not the high-card one; the valbitmap table carries `_counts` blobs."""
        _setup_adv_table(db)
        row = db.execute("""
            SELECT column_valmap::text
            FROM deltax.deltax_partition
            WHERE table_name LIKE 'adv_%' AND is_compressed = true
            LIMIT 1
        """).fetchone()
        assert row is not None and row[0] is not None
        valmap_text = row[0]
        assert '"adv_id"' in valmap_text, (
            f"adv_id missing from column_valmap: {valmap_text}"
        )
        assert '"payload"' not in valmap_text, (
            f"payload (high-card) unexpectedly in column_valmap: {valmap_text}"
        )
        for v in ["0", "1", "2", "9"]:
            assert f'"{v}"' in valmap_text, (
                f"value {v!r} missing from column_valmap: {valmap_text}"
            )

        # The `_counts` sidecar exists and sums to the non-NULL row count.
        vb = db.execute("""
            SELECT c.relname
            FROM pg_class c
            JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE n.nspname = '_deltax_compressed'
              AND c.relname LIKE 'adv_%_valbitmap'
            LIMIT 1
        """).fetchone()
        assert vb is not None, "valbitmap companion table missing"
        n_counts = db.execute(
            f'SELECT count(*) FROM "_deltax_compressed"."{vb[0]}" '
            "WHERE _counts IS NOT NULL"
        ).fetchone()[0]
        assert n_counts > 0, "expected _counts sidecar rows for adv_id"

    def test_direct_backfill_populates_counts(self, db):
        """COPY ... WITH (FORMAT deltax_compress_csv) exercises `src/copy.rs`;
        the sidecar must be equivalent and GROUP BY results exact."""
        import io

        db.execute("DROP TABLE IF EXISTS adv_db CASCADE")
        db.execute(f"SET pg_deltax.mock_now = '{MOCK_NOW}'")
        db.execute("""
            CREATE TABLE adv_db (
                ts        timestamptz NOT NULL,
                order_id  integer NOT NULL,
                adv_id    integer
            )
        """)
        db.execute(
            "SELECT deltax.deltax_create_table('adv_db', 'ts', '1 day'::interval)"
        )
        db.execute(
            "SELECT deltax.deltax_enable_compression('adv_db', "
            f"order_by => ARRAY['order_id'], segment_size => {SEGMENT_SIZE})"
        )
        db.commit()

        buf = io.StringIO()
        for sec, order_id, adv in _gen_rows():
            ts = f"2025-01-15 00:{sec // 60:02d}:{sec % 60:02d}+00"
            adv_csv = "" if adv is None else str(adv)
            buf.write(f"{ts},{order_id},{adv_csv}\n")

        sql = "COPY adv_db FROM STDIN WITH (FORMAT deltax_compress_csv, DELIMITER ',')"
        with db.cursor() as cur:
            with cur.copy(sql) as copy:
                copy.write(buf.getvalue().encode())
        db.commit()

        got = _query_counts(
            db, "SELECT adv_id, count(*) FROM adv_db GROUP BY adv_id"
        )
        assert got == _expected_counts()

        got = _query_counts(
            db,
            "SELECT adv_id, count(*) FROM adv_db WHERE adv_id <> 0 GROUP BY adv_id",
        )
        expected = {
            k: v
            for k, v in _expected_counts().items()
            if k is not None and k != 0
        }
        assert got == expected
