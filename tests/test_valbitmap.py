"""End-to-end tests for the per-segment value-presence bitmap.

The bitmap targets text equality predicates on low-cardinality columns
(ndistinct ≤ 32). It records, per segment, which of the partition-level
distinct values appear — letting `WHERE col = const` skip segments whose
bit for `const` is clear, with no false positives.

These tests exercise the full flow: INSERT → enable_compression →
compress_partition → query → inspect `segments_valbitmap_skipped` in
EXPLAIN's `DeltaX Stats:` line.
"""

import re
import pytest

# `pg_deltax.mock_now` pins the partition origin so all our test data
# falls into partitions deltax_create_table actually creates.
MOCK_NOW = "2025-01-15 12:00:00+00"
BASE_TS = "2025-01-15 00:00:00+00"


def _setup_event_table(conn, n_segments=4, segment_size=200):
    """Create a deltax table with a low-cardinality `event_type` column.

    Layout:
      - n_segments segments × segment_size rows each.
      - 'common' appears in every segment.
      - 'rare' appears only in segment 0.
      - 'never' is never inserted (used to exercise the prune-all path).

    All rows go into a single partition (one calendar day) so we get
    exactly n_segments segments after compression.
    """
    conn.execute("DROP TABLE IF EXISTS evt CASCADE")
    conn.execute(f"SET pg_deltax.mock_now = '{MOCK_NOW}'")
    conn.execute("""
        CREATE TABLE evt (
            ts          timestamptz NOT NULL,
            order_id    integer NOT NULL,
            event_type  text NOT NULL,
            payload     text
        )
    """)
    conn.execute(
        "SELECT deltax.deltax_create_table('evt', 'ts', '1 day'::interval)"
    )
    conn.commit()

    base = BASE_TS
    rows = []
    for s in range(n_segments):
        for i in range(segment_size):
            order_id = s * segment_size + i
            # ts spreads across the day so all rows fall in one partition.
            ts = (
                f"'{base}'::timestamptz + "
                f"interval '{s * segment_size + i} seconds'"
            )
            # Distribute event_types so:
            #   - 'common' appears in every segment
            #   - 'meh'    appears in every segment
            #   - 'middle' appears in every segment except the last
            #   - 'rare'   appears ONLY in segment 0
            if s == 0 and i % 25 == 0:
                et = "rare"
            elif s < n_segments - 1 and i == 7:
                et = "middle"
            elif i % 2 == 0:
                et = "common"
            else:
                et = "meh"
            # `payload` is high-cardinality (segment_size distinct values
            # per segment); should NOT get a bitmap.
            rows.append(f"({ts}, {order_id}, '{et}', 'p{i}')")

    # Insert in batches.
    batch = 500
    for i in range(0, len(rows), batch):
        conn.execute(
            "INSERT INTO evt (ts, order_id, event_type, payload) VALUES "
            + ", ".join(rows[i:i + batch])
        )
    conn.commit()

    # Enable compression with order_by `order_id` so segments are sorted by
    # order_id (matching the RTABench layout) — and small segment_size so
    # we get the requested n_segments.
    conn.execute(
        "SELECT deltax.deltax_enable_compression('evt', "
        f"order_by => ARRAY['order_id'], segment_size => {segment_size})"
    )
    conn.commit()

    # Compress all non-empty, non-default partitions.
    parts = conn.execute(
        "SELECT partition_name FROM deltax.deltax_partition_info('evt') "
        "WHERE partition_name NOT LIKE '%default%'"
    ).fetchall()
    for (part_name,) in parts:
        n = conn.execute(f'SELECT count(*) FROM "{part_name}"').fetchone()[0]
        if n == 0:
            continue
        conn.execute(f"SELECT deltax.deltax_compress_partition('{part_name}')")
    conn.commit()


_VB_SKIP_RE = re.compile(r"segments_valbitmap_skipped=(\d+)")
_SEGS_RE = re.compile(r"\bsegments=(\d+)\s")


def _explain_skip_counts(conn, sql):
    """Run EXPLAIN (ANALYZE, BUFFERS, TIMING OFF) and return
    (segments_decompressed, segments_valbitmap_skipped) extracted from the
    DeltaX Stats line. Asserts the line is present."""
    rows = conn.execute(
        f"EXPLAIN (ANALYZE, BUFFERS, TIMING OFF) {sql}"
    ).fetchall()
    text = "\n".join(r[0] for r in rows)
    assert "DeltaX Stats" in text, f"no DeltaX Stats in:\n{text}"
    vb_match = _VB_SKIP_RE.search(text)
    assert vb_match, f"segments_valbitmap_skipped missing:\n{text}"
    segs_match = _SEGS_RE.search(text)
    assert segs_match, f"segments=N missing:\n{text}"
    return int(segs_match.group(1)), int(vb_match.group(1))


class TestValbitmap:
    # Use `SELECT * ... LIMIT 1000` (not count(*)) so the planner picks
    # `Custom Scan (DeltaXAppend)` rather than the `DeltaXAgg` aggregate
    # fast-path. Only DeltaXAppend goes through `load_segments_heap` (and
    # thus the valbitmap pruning loop) and surfaces segments_valbitmap_skipped
    # in its DeltaX Stats line.

    def test_rare_value_skips_most_segments(self, db):
        """`event_type='rare'` exists in segment 0 only → others get skipped."""
        _setup_event_table(db, n_segments=4)
        decomp, vb_skipped = _explain_skip_counts(
            db,
            "SELECT * FROM evt WHERE event_type = 'rare' LIMIT 1000",
        )
        # 1 segment holds 'rare', the other 3 get pruned.
        assert vb_skipped >= 1, (
            f"expected segments_valbitmap_skipped > 0 for rare value, "
            f"got {vb_skipped} (decompressed={decomp})"
        )
        # Result correctness: at least one 'rare' row.
        n = db.execute(
            "SELECT count(*) FROM evt WHERE event_type = 'rare'"
        ).fetchone()[0]
        assert n > 0

    def test_common_value_no_skip(self, db):
        """`event_type='common'` exists in every segment → no skip."""
        _setup_event_table(db, n_segments=4)
        _, vb_skipped = _explain_skip_counts(
            db,
            "SELECT * FROM evt WHERE event_type = 'common' LIMIT 1000",
        )
        assert vb_skipped == 0, (
            f"expected segments_valbitmap_skipped == 0 for common value, "
            f"got {vb_skipped}"
        )

    def test_unknown_value_skips_everything(self, db):
        """`event_type='never'` not present anywhere → ALL segments skipped
        without even reading the bitmap (prune_all path)."""
        _setup_event_table(db, n_segments=4)
        decomp, vb_skipped = _explain_skip_counts(
            db,
            "SELECT * FROM evt WHERE event_type = 'never' LIMIT 1000",
        )
        assert decomp == 0, (
            f"expected segments=0 for unknown value, got {decomp}"
        )
        assert vb_skipped >= 4, (
            f"expected all 4 segments skipped for unknown value, "
            f"got {vb_skipped}"
        )
        n = db.execute(
            "SELECT count(*) FROM evt WHERE event_type = 'never'"
        ).fetchone()[0]
        assert n == 0

    def test_high_cardinality_column_no_bitmap(self, db):
        """A column with > VALBITMAP_MAX_DISTINCT (32) distinct values
        should not get a bitmap entry. Equality on it falls back to batch
        filter — no segments_valbitmap_skipped contribution."""
        _setup_event_table(db, n_segments=4)
        # `payload` ranges over 200 distinct values per segment → not
        # eligible for the bitmap. Even when the value DOES exist in the
        # data, valbitmap_skipped should remain 0 because there's no
        # bitmap entry to consult.
        _, vb_skipped = _explain_skip_counts(
            db,
            "SELECT * FROM evt WHERE payload = 'p7' LIMIT 1000",
        )
        assert vb_skipped == 0, (
            f"expected segments_valbitmap_skipped == 0 for high-card "
            f"column, got {vb_skipped}"
        )

    def test_direct_backfill_populates_valbitmap(self, db):
        """`COPY ... WITH (FORMAT deltax_compress_csv)` exercises a separate
        code path (`src/copy.rs`) than `deltax_compress_partition`. Both
        should produce the same valbitmap state. This test mirrors the
        rare/common/never assertions but loads via direct backfill."""
        import io
        db.execute("DROP TABLE IF EXISTS evt_db CASCADE")
        db.execute(f"SET pg_deltax.mock_now = '{MOCK_NOW}'")
        db.execute("""
            CREATE TABLE evt_db (
                ts          timestamptz NOT NULL,
                order_id    integer NOT NULL,
                event_type  text NOT NULL
            )
        """)
        db.execute(
            "SELECT deltax.deltax_create_table('evt_db', 'ts', '1 day'::interval)"
        )
        db.execute(
            "SELECT deltax.deltax_enable_compression('evt_db', "
            "order_by => ARRAY['order_id'], segment_size => 200)"
        )
        db.commit()

        # Generate 4 segments × 200 rows in CSV form. Same distribution as
        # _setup_event_table: 'rare' only in segment 0.
        n_segments = 4
        segment_size = 200
        buf = io.StringIO()
        for s in range(n_segments):
            for i in range(segment_size):
                order_id = s * segment_size + i
                ts_offset = s * segment_size + i
                ts = f"2025-01-15 00:00:{ts_offset // 60:02d}:{ts_offset % 60:02d}+00"
                if s == 0 and i % 25 == 0:
                    et = "rare"
                elif i % 2 == 0:
                    et = "common"
                else:
                    et = "meh"
                buf.write(f"{ts},{order_id},{et}\n")

        sql = "COPY evt_db FROM STDIN WITH (FORMAT deltax_compress_csv, DELIMITER ',')"
        with db.cursor() as cur:
            with cur.copy(sql) as copy:
                copy.write(buf.getvalue().encode())
        db.commit()

        # Verify column_valmap is populated.
        row = db.execute("""
            SELECT column_valmap::text
            FROM deltax.deltax_partition
            WHERE table_name LIKE 'evt_db_%' AND is_compressed = true
            LIMIT 1
        """).fetchone()
        assert row is not None and row[0] is not None
        for v in ["common", "meh", "rare"]:
            assert f'"{v}"' in row[0], f"value {v!r} missing: {row[0]}"

        # Rare value should skip segments at query time.
        rows_explain = db.execute(
            "EXPLAIN (ANALYZE, BUFFERS, TIMING OFF) "
            "SELECT * FROM evt_db WHERE event_type = 'rare' LIMIT 1000"
        ).fetchall()
        text = "\n".join(r[0] for r in rows_explain)
        match = _VB_SKIP_RE.search(text)
        assert match, f"segments_valbitmap_skipped missing:\n{text}"
        assert int(match.group(1)) >= 1, (
            f"expected vb_skipped > 0, got {match.group(1)}"
        )

    def test_overflowed_segment_survives_valmap_miss(self, db):
        """Regression: a segment that exceeds VALBITMAP_MAX_DISTINCT (32)
        writes no valbitmap row and contributes nothing to the partition
        valmap. Querying a value that lives ONLY in that segment used to hit
        the "constant absent from the valmap → prune every segment" shortcut
        and return zero rows. The valmap-miss path may only skip segments
        that wrote a bitmap row — never the overflowed one."""
        db.execute("DROP TABLE IF EXISTS evt_ov CASCADE")
        db.execute(f"SET pg_deltax.mock_now = '{MOCK_NOW}'")
        db.execute("""
            CREATE TABLE evt_ov (
                ts          timestamptz NOT NULL,
                order_id    integer NOT NULL,
                event_type  text NOT NULL
            )
        """)
        db.execute(
            "SELECT deltax.deltax_create_table('evt_ov', 'ts', '1 day'::interval)"
        )
        db.commit()

        segment_size = 200
        rows = []
        # Segment 0 (order_id 0..199): 40 distinct values 'ov00'..'ov39' —
        # exceeds the 32-distinct cap, so this segment gets NO bitmap row
        # and its values are missing from the partition valmap. 'ov07'
        # exists only here (order_id 7, 47, 87, 127, 167).
        for i in range(segment_size):
            ts = f"'{BASE_TS}'::timestamptz + interval '{i} seconds'"
            rows.append(f"({ts}, {i}, 'ov{i % 40:02d}')")
        # Segment 1 (order_id 200..399): 2 distinct values → bitmap row
        # written; the partition valmap ends up ['aaa', 'zzz'] only. The
        # values straddle the 'ov*' band so segment 1's [min,max] range
        # covers the probe constants below — text minmax pruning stays out
        # of the picture and only the valbitmap decides.
        for i in range(segment_size):
            order_id = segment_size + i
            ts = f"'{BASE_TS}'::timestamptz + interval '{order_id} seconds'"
            et = "aaa" if i % 2 == 0 else "zzz"
            rows.append(f"({ts}, {order_id}, '{et}')")
        db.execute(
            "INSERT INTO evt_ov (ts, order_id, event_type) VALUES "
            + ", ".join(rows)
        )
        db.commit()

        db.execute(
            "SELECT deltax.deltax_enable_compression('evt_ov', "
            f"order_by => ARRAY['order_id'], segment_size => {segment_size})"
        )
        db.commit()
        parts = db.execute(
            "SELECT partition_name FROM deltax.deltax_partition_info('evt_ov') "
            "WHERE partition_name NOT LIKE '%default%'"
        ).fetchall()
        for (part_name,) in parts:
            n = db.execute(f'SELECT count(*) FROM "{part_name}"').fetchone()[0]
            if n:
                db.execute(
                    f"SELECT deltax.deltax_compress_partition('{part_name}')"
                )
        db.commit()

        # Sanity: the partition valmap must NOT contain the overflowed
        # segment's values — that's exactly the shape that triggered the bug.
        valmap = db.execute("""
            SELECT column_valmap::text
            FROM deltax.deltax_partition
            WHERE table_name LIKE 'evt_ov_%' AND is_compressed = true
        """).fetchone()[0]
        assert valmap is not None and '"aaa"' in valmap
        assert '"ov07"' not in valmap, (
            f"expected overflowed segment's values absent from valmap, "
            f"got: {valmap}"
        )

        # The rows in the overflowed segment must come back. Plan shape:
        # the bitmap-covered segment is skipped via its bitmap row
        # (vb_skipped=1; 'ov07' misses the ['aaa','zzz'] valmap and segment
        # 1's [min,max] covers 'ov07', so only the valbitmap can skip it)
        # while the overflowed segment — with NO bitmap row — must be
        # decompressed and batch-filtered. Pre-fix, the valmap miss pruned
        # BOTH segments without reading the bitmap table → zero rows.
        decomp, vb_skipped = _explain_skip_counts(
            db,
            "SELECT * FROM evt_ov WHERE event_type = 'ov07' LIMIT 1000",
        )
        assert vb_skipped == 1, (
            f"expected exactly the bitmap-covered segment skipped, "
            f"got vb_skipped={vb_skipped}"
        )
        assert decomp == 1, (
            f"expected the overflowed segment to be scanned, got "
            f"segments={decomp}"
        )
        got = [
            r[0]
            for r in db.execute(
                "SELECT order_id FROM evt_ov WHERE event_type = 'ov07' "
                "ORDER BY order_id"
            ).fetchall()
        ]
        assert got == [7, 47, 87, 127, 167], (
            f"overflowed segment was wrongly pruned, got rows: {got}"
        )

        # A value present nowhere still returns nothing. (No plan-shape
        # assertion here: the overflowed segment may legitimately be
        # skipped by exact per-segment evidence at decompress time, e.g.
        # the dictionary codec's value check.)
        n = db.execute(
            "SELECT count(*) FROM evt_ov WHERE event_type = 'ov0a'"
        ).fetchone()[0]
        assert n == 0

    def test_catalog_column_valmap_populated(self, db):
        """After compression, deltax.deltax_partition.column_valmap should hold
        the sorted value list for the low-cardinality `event_type`
        column and should NOT include `payload` (>32 distinct values)."""
        _setup_event_table(db, n_segments=4)
        row = db.execute("""
            SELECT column_valmap::text
            FROM deltax.deltax_partition
            WHERE table_name LIKE 'evt_%' AND is_compressed = true
            LIMIT 1
        """).fetchone()
        assert row is not None
        valmap_text = row[0]
        # event_type should be in the map; payload should not.
        assert '"event_type"' in valmap_text, (
            f"event_type missing from column_valmap: {valmap_text}"
        )
        assert '"payload"' not in valmap_text, (
            f"payload (high-card) unexpectedly in column_valmap: {valmap_text}"
        )
        # Check that the values 'common', 'meh', 'middle', 'rare' all show up.
        for v in ["common", "meh", "middle", "rare"]:
            assert f'"{v}"' in valmap_text, (
                f"value {v!r} missing from column_valmap: {valmap_text}"
            )
