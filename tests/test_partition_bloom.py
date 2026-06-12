"""End-to-end tests for partition-level bloom sentinels.

During compression, each high-cardinality numeric column gets a coarse
partition-wide bloom filter stored as a `_segment_id = -1` sentinel row in
the partition's `_blooms` companion table. At query time the sentinel is
the first row a forward PK scan returns; if it rejects every probe value,
every segment in the partition is pruned without reading any per-segment
bloom row.

These tests exercise the full flow: INSERT → enable_compression →
compress_partition → query, for both the SPI compress path and the
direct-backfill COPY path.

Data layout trick: `user_id = i*10 + day_offset` gives the three daily
partitions fully overlapping [min, max] ranges (so min/max pruning cannot
reject a partition) while keeping their actual value sets disjoint (so the
partition bloom can).
"""

import re

MOCK_NOW = "2025-01-15 12:00:00+00"
DAYS = ["2025-01-14", "2025-01-15", "2025-01-16"]
N_PER_DAY = 600  # 3 segments × 200 rows per daily partition
SEGMENT_SIZE = 200

_BLOOM_SKIP_RE = re.compile(r"segments_bloom_skipped=(\d+)")
_SEGS_RE = re.compile(r"\bsegments=(\d+)\s")


def _setup_table(conn):
    """3 daily partitions × 3 segments × 200 rows.

    Columns:
      - user_id bigint: near-unique → useful sentinel.
      - status  int:    5 distinct values → tiny (folded) sentinel.
      - payload text:   no blooms at all (text).
    Day d's user_ids are ≡ d (mod 10), so the partitions' value sets are
    disjoint while their ranges fully overlap.
    """
    conn.execute("DROP TABLE IF EXISTS evt CASCADE")
    conn.execute(f"SET pg_deltax.mock_now = '{MOCK_NOW}'")
    conn.execute("""
        CREATE TABLE evt (
            ts       timestamptz NOT NULL,
            user_id  bigint NOT NULL,
            status   integer NOT NULL,
            payload  text
        )
    """)
    conn.execute("SELECT deltax.deltax_create_table('evt', 'ts', '1 day'::interval)")
    conn.commit()

    for d, day in enumerate(DAYS):
        rows = []
        for i in range(N_PER_DAY):
            ts = f"'{day} 00:00:00+00'::timestamptz + interval '{i} seconds'"
            rows.append(f"({ts}, {i * 10 + d}, {i % 5}, 'p{i}')")
        conn.execute(
            "INSERT INTO evt (ts, user_id, status, payload) VALUES " + ", ".join(rows)
        )
    conn.commit()

    conn.execute(
        "SELECT deltax.deltax_enable_compression('evt', "
        f"order_by => ARRAY['user_id'], segment_size => {SEGMENT_SIZE})"
    )
    conn.commit()

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


def _blooms_tables(conn, prefix="evt"):
    """Fully-qualified names of the partitions' blooms companion tables."""
    rows = conn.execute(
        "SELECT n.nspname, c.relname FROM pg_class c "
        "JOIN pg_namespace n ON n.oid = c.relnamespace "
        f"WHERE c.relname LIKE '{prefix}\\_%\\_blooms' AND c.relkind = 'r'"
    ).fetchall()
    return [f'"{ns}"."{rel}"' for ns, rel in rows]


def _explain_bloom_skips(conn, sql):
    rows = conn.execute(f"EXPLAIN (ANALYZE, BUFFERS, TIMING OFF) {sql}").fetchall()
    text = "\n".join(r[0] for r in rows)
    assert "DeltaX Stats" in text, f"no DeltaX Stats in:\n{text}"
    m = _BLOOM_SKIP_RE.search(text)
    assert m, f"segments_bloom_skipped missing:\n{text}"
    s = _SEGS_RE.search(text)
    assert s, f"segments=N missing:\n{text}"
    return int(s.group(1)), int(m.group(1))


class TestPartitionBloom:
    def test_sentinel_rows_created(self, db):
        """Every compressed partition gets one sentinel row per
        bloom-supported column (ts, user_id, status — not text), folded to
        ~10 bits/element: low-cardinality `status` folds to the 64-byte
        minimum while near-unique `user_id` stays larger."""
        _setup_table(db)
        tables = _blooms_tables(db)
        assert len(tables) == 3, f"expected 3 blooms tables, got {tables}"
        for t in tables:
            n_sentinel_cols = db.execute(
                f"SELECT count(DISTINCT _col_idx) FROM {t} WHERE _segment_id = -1"
            ).fetchone()[0]
            n_segment_cols = db.execute(
                f"SELECT count(DISTINCT _col_idx) FROM {t} WHERE _segment_id >= 0"
            ).fetchone()[0]
            assert n_sentinel_cols == n_segment_cols == 3, (
                f"expected sentinels for all 3 bloom-supported columns in {t}: "
                f"{n_sentinel_cols} sentinel cols vs {n_segment_cols} segment cols"
            )
            sizes = dict(
                db.execute(
                    f"SELECT _col_idx, length(_data) FROM {t} WHERE _segment_id = -1"
                ).fetchall()
            )
            # 600 distinct user_ids → > 64B; 5 distinct statuses → folds to 64B.
            assert max(sizes.values()) > 64, f"no folded-large sentinel in {t}: {sizes}"
            assert min(sizes.values()) == 64, f"no folded-small sentinel in {t}: {sizes}"

    def test_absent_value_pruned_by_sentinel_alone(self, db):
        """Prove the sentinel itself fires: delete every per-segment bloom
        row (keeping only sentinels). Any bloom pruning that remains can
        only come from the partition-level filter."""
        _setup_table(db)
        for t in _blooms_tables(db):
            db.execute(f"DELETE FROM {t} WHERE _segment_id >= 0")
        db.commit()

        # 3005 ≡ 5 (mod 10) → exists in no partition, but lies inside every
        # partition's [min, max] range, so min/max pruning leaves at least
        # one segment per partition for the bloom phase.
        _, bloom_skipped = _explain_bloom_skips(
            db, "SELECT * FROM evt WHERE user_id = 3005 LIMIT 100"
        )
        assert bloom_skipped >= 3, (
            f"sentinels should prune ≥1 minmax-surviving segment per "
            f"partition, got segments_bloom_skipped={bloom_skipped}"
        )
        n = db.execute("SELECT count(*) FROM evt WHERE user_id = 3005").fetchone()[0]
        assert n == 0

        # With the GUC off the sentinel is ignored, and the per-segment rows
        # are gone — so no bloom pruning at all. Results stay correct.
        db.execute("SET pg_deltax.partition_bloom_filters = off")
        _, bloom_skipped_off = _explain_bloom_skips(
            db, "SELECT * FROM evt WHERE user_id = 3005 LIMIT 100"
        )
        assert bloom_skipped_off == 0, (
            f"GUC off should disable sentinel pruning, got {bloom_skipped_off}"
        )
        n = db.execute("SELECT count(*) FROM evt WHERE user_id = 3005").fetchone()[0]
        assert n == 0
        db.execute("RESET pg_deltax.partition_bloom_filters")

    def test_present_value_not_pruned(self, db):
        """A value that exists in exactly one partition must survive the
        sentinel of its own partition and return the right rows."""
        _setup_table(db)
        # 3001 = 300*10 + 1 → exists only in day 2025-01-15 (offset 1).
        for guc in ("on", "off"):
            db.execute(f"SET pg_deltax.partition_bloom_filters = {guc}")
            rows = db.execute(
                "SELECT user_id, status FROM evt WHERE user_id = 3001"
            ).fetchall()
            assert rows == [(3001, 0)], f"guc={guc}: {rows}"
        db.execute("RESET pg_deltax.partition_bloom_filters")

    def test_in_list_probe(self, db):
        """IN lists probe the sentinel with every constant; the partition
        passes if any constant might be present."""
        _setup_table(db)
        rows = db.execute(
            "SELECT user_id FROM evt WHERE user_id IN (3001, 3005, 4002) ORDER BY user_id"
        ).fetchall()
        assert rows == [(3001,), (4002,)], rows

    def test_decompress_recompress_rebuilds_sentinels(self, db):
        """Decompress drops the blooms companion table — sentinel included —
        so a stale sentinel can never survive a decompress → modify →
        recompress cycle. Recompression rebuilds the sentinel over the new
        value set (fail-safe: absence of a sentinel just means no skipping)."""
        _setup_table(db)
        part = db.execute(
            "SELECT partition_name FROM deltax.deltax_partition_info('evt') "
            "WHERE partition_name NOT LIKE '%default%' ORDER BY partition_name LIMIT 1"
        ).fetchone()[0]
        db.execute(f"SELECT deltax.deltax_decompress_partition('{part}')")
        db.commit()
        assert len(_blooms_tables(db)) == 2, (
            "decompress must drop the partition's blooms table (and its sentinel)"
        )

        # 9999 ≡ 9 (mod 10): absent from every partition before this insert,
        # so the original sentinel would have rejected it.
        db.execute(
            f"INSERT INTO \"{part}\" (ts, user_id, status, payload) "
            f"VALUES ('{DAYS[0]} 11:00:00+00', 9999, 1, 'new')"
        )
        db.execute(f"SELECT deltax.deltax_compress_partition('{part}')")
        db.commit()
        assert len(_blooms_tables(db)) == 3

        rows = db.execute(
            "SELECT user_id, status FROM evt WHERE user_id = 9999"
        ).fetchall()
        assert rows == [(9999, 1)], f"rebuilt sentinel must admit the new value: {rows}"

    def test_direct_backfill_creates_sentinels(self, db):
        """COPY ... (FORMAT deltax_compress_csv) exercises src/copy.rs —
        a separate build path that must produce the same sentinel state."""
        import io

        db.execute("DROP TABLE IF EXISTS evt_db CASCADE")
        db.execute(f"SET pg_deltax.mock_now = '{MOCK_NOW}'")
        db.execute("""
            CREATE TABLE evt_db (
                ts       timestamptz NOT NULL,
                user_id  bigint NOT NULL,
                status   integer NOT NULL
            )
        """)
        db.execute(
            "SELECT deltax.deltax_create_table('evt_db', 'ts', '1 day'::interval)"
        )
        db.execute(
            "SELECT deltax.deltax_enable_compression('evt_db', "
            f"order_by => ARRAY['user_id'], segment_size => {SEGMENT_SIZE})"
        )
        db.commit()

        buf = io.StringIO()
        for d, day in enumerate(DAYS):
            for i in range(N_PER_DAY):
                buf.write(
                    f"{day} 00:{i // 60:02d}:{i % 60:02d}+00,{i * 10 + d},{i % 5}\n"
                )
        sql = "COPY evt_db FROM STDIN WITH (FORMAT deltax_compress_csv, DELIMITER ',')"
        with db.cursor() as cur:
            with cur.copy(sql) as copy:
                copy.write(buf.getvalue().encode())
        db.commit()

        tables = _blooms_tables(db, prefix="evt_db")
        assert len(tables) == 3, f"expected 3 blooms tables, got {tables}"
        for t in tables:
            n_sentinel_cols = db.execute(
                f"SELECT count(DISTINCT _col_idx) FROM {t} WHERE _segment_id = -1"
            ).fetchone()[0]
            n_segment_cols = db.execute(
                f"SELECT count(DISTINCT _col_idx) FROM {t} WHERE _segment_id >= 0"
            ).fetchone()[0]
            assert n_sentinel_cols == n_segment_cols == 3, (
                f"expected sentinels for all 3 bloom-supported columns in "
                f"direct backfill for {t}: {n_sentinel_cols} vs {n_segment_cols}"
            )

        # Absent-everywhere value: correct empty result.
        n = db.execute(
            "SELECT count(*) FROM evt_db WHERE user_id = 3005"
        ).fetchone()[0]
        assert n == 0
        # Present value: exactly one row.
        n = db.execute(
            "SELECT count(*) FROM evt_db WHERE user_id = 3001"
        ).fetchone()[0]
        assert n == 1
