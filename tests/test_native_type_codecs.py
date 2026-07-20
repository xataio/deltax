"""Integration tests for the native type codecs (TYPE_SUPPORT_PLAN Phase 2).

time/uuid/bytea/inet/cidr use native binary codecs; numeric uses the
scaled-i64 codec with a per-blob text escape hatch. Legacy partitions
compressed via the ::text fallback must keep reading correctly — dispatch is
per blob through the compression type tag, exercised here by compressing one
partition with `pg_deltax.force_text_fallback = on` (legacy generation) and
one with it off (native generation) in the same table.

Also holds the regression test for the count(*) FILTER fast-path bug (the
single-count(*) DeltaXCount path used to ignore the FILTER clause entirely).
"""

# Partition pre-creation covers now-1day .. now+premake, so this gives
# partitions for Jan 14, 15, and 16.
MOCK_NOW = "2025-01-15 12:00:00+00"

SNAPSHOT_SQL = """
    SELECT n,
           tv::text,
           u::text,
           b::text,
           ip::text, host(ip), family(ip),
           cd::text,
           amt::text,
           wild::text
    FROM {table}
    ORDER BY n
"""


def setup_table(conn, table_name="native_types"):
    conn.execute(f"SET pg_deltax.mock_now = '{MOCK_NOW}'")
    conn.execute(f"""
        CREATE TABLE {table_name} (
            ts TIMESTAMPTZ NOT NULL,
            n INTEGER NOT NULL,
            tv TIME,
            u UUID,
            b BYTEA,
            ip INET,
            cd CIDR,
            amt NUMERIC(12,2),
            wild NUMERIC
        )
    """)
    conn.execute(
        f"SELECT deltax.deltax_create_table('{table_name}', 'ts', '1 day'::interval)"
    )
    conn.execute(
        f"SELECT deltax.deltax_enable_compression('{table_name}', "
        "order_by => ARRAY['ts'])"
    )
    conn.commit()


def insert_rows(conn, day, n_offset, n_rows=150, table_name="native_types"):
    """Insert n_rows into partition day `day` (e.g. '2025-01-15').

    Every type gets NULLs mixed in; `wild` mixes NaN and >i64 mantissas so
    some numeric blobs take the text escape hatch even in native generation.
    """
    values = []
    for i in range(n_rows):
        g = n_offset + i
        ts = f"'{day}'::timestamptz + interval '{i} minutes'"
        tv = "NULL" if g % 5 == 4 else f"'04:05:06.00700'::time + ({g} || ' sec')::interval"
        u = "NULL" if g % 7 == 6 else f"'00000000-0000-4000-8000-{g:012d}'::uuid"
        b = "NULL" if g % 6 == 5 else f"decode(lpad(to_hex({g} * 257), 6, '0') || '00ff', 'hex')"
        ip = (
            "NULL"
            if g % 8 == 7
            else (
                f"'10.0.{g // 250}.{g % 250 + 1}'::inet"
                if g % 2 == 0
                else f"'2001:db8::{g + 1:x}'::inet"
            )
        )
        cd = "NULL" if g % 4 == 3 else f"'192.168.{g % 100}.0/24'::cidr"
        amt = "NULL" if g % 5 == 4 else f"({g} * 1.37 - 50)::numeric(12,2)"
        wild = (
            "'NaN'::numeric"
            if g % 10 == 0
            else (
                f"('9' || repeat('8', 25) || '.5')::numeric"
                if g % 10 == 1
                else f"({g} * 0.001)::numeric(10,3)"
            )
        )
        values.append(f"({ts}, {g}, {tv}, {u}, {b}, {ip}, {cd}, {amt}, {wild})")
    conn.execute(
        f"INSERT INTO {table_name} (ts, n, tv, u, b, ip, cd, amt, wild) VALUES "
        + ", ".join(values)
    )
    conn.commit()


def partition_for(conn, day, table_name="native_types"):
    rows = conn.execute(
        f"SELECT partition_name FROM deltax.deltax_partition_info('{table_name}') "
        f"WHERE range_start <= '{day}'::timestamptz "
        f"AND range_end > '{day}'::timestamptz"
    ).fetchall()
    assert len(rows) == 1
    return rows[0][0]


def compress(conn, part_name, table_name="native_types"):
    result = conn.execute(
        f"SELECT deltax.deltax_compress_partition('{part_name}')"
    ).fetchone()[0]
    conn.commit()
    assert "Compressed" in result


def blob_tags(conn, part_name):
    """Distinct compression-type tag bytes across the partition's blobs."""
    rows = conn.execute(
        f'SELECT DISTINCT get_byte(_data, 0) FROM _deltax_compressed."{part_name}_blobs"'
    ).fetchall()
    return {r[0] for r in rows}


class TestNativeTypeCodecs:
    def test_roundtrip_and_operators(self, db):
        setup_table(db)
        insert_rows(db, "2025-01-15", 0)

        before = db.execute(SNAPSHOT_SQL.format(table="native_types")).fetchall()
        ops_sql = """
            SELECT count(*) FILTER (WHERE tv > '04:06:00'::time),
                   count(*) FILTER (WHERE u = '00000000-0000-4000-8000-000000000010'::uuid),
                   count(*) FILTER (WHERE length(b) = 5),
                   count(*) FILTER (WHERE ip <<= '10.0.0.0/8'::cidr),
                   sum(amt)::text,
                   count(*) FILTER (WHERE wild = 'NaN'::numeric)
            FROM native_types
        """
        ops_before = db.execute(ops_sql).fetchall()

        part = partition_for(db, "2025-01-15")
        compress(db, part)

        # Native generation must include the new tags: 10-12 (binary) and/or
        # 13 (NumericScaled) plus integer tags for time.
        tags = blob_tags(db, part)
        assert tags & {10, 11, 12, 13}, f"expected native tags, got {tags}"

        assert db.execute(SNAPSHOT_SQL.format(table="native_types")).fetchall() == before
        assert db.execute(ops_sql).fetchall() == ops_before

        result = db.execute(
            f"SELECT deltax.deltax_decompress_partition('{part}')"
        ).fetchone()[0]
        db.commit()
        assert "Decompressed" in result
        assert db.execute(SNAPSHOT_SQL.format(table="native_types")).fetchall() == before

    def test_mixed_generation_partitions(self, db):
        """One legacy-format partition (force_text_fallback=on) + one native
        partition in the same table: reads across both, per-partition tag
        verification, and decompress of both generations."""
        setup_table(db)
        insert_rows(db, "2025-01-14", 0)
        insert_rows(db, "2025-01-15", 1000)

        before = db.execute(SNAPSHOT_SQL.format(table="native_types")).fetchall()
        assert len(before) == 300

        legacy_part = partition_for(db, "2025-01-14")
        native_part = partition_for(db, "2025-01-15")

        db.execute("SET pg_deltax.force_text_fallback = on")
        compress(db, legacy_part)
        db.execute("SET pg_deltax.force_text_fallback = off")
        compress(db, native_part)

        legacy_tags = blob_tags(db, legacy_part)
        native_tags = blob_tags(db, native_part)
        assert not legacy_tags & {10, 11, 12, 13}, (
            f"legacy partition must only use text-era tags, got {legacy_tags}"
        )
        assert native_tags & {10, 11, 12, 13}, (
            f"native partition should use new tags, got {native_tags}"
        )

        # Cross-generation read: both partitions in one scan.
        assert db.execute(SNAPSHOT_SQL.format(table="native_types")).fetchall() == before

        # Typed predicates spanning both generations.
        cnt = db.execute(
            "SELECT count(*) FROM native_types WHERE ip <<= '10.0.0.0/8'::cidr"
        ).fetchone()[0]
        assert cnt > 0

        # Decompress both generations and re-verify.
        for part in (legacy_part, native_part):
            result = db.execute(
                f"SELECT deltax.deltax_decompress_partition('{part}')"
            ).fetchone()[0]
            db.commit()
            assert "Decompressed" in result
        assert db.execute(SNAPSHOT_SQL.format(table="native_types")).fetchall() == before


class TestCountFilterFastPath:
    def test_single_count_filter_not_fast_pathed(self, db):
        """Regression: a lone count(*) FILTER (WHERE ...) used to be answered
        from catalog row counts, ignoring the FILTER entirely."""
        setup_table(db)
        insert_rows(db, "2025-01-15", 0)

        expected = db.execute(
            "SELECT count(*) FILTER (WHERE n > 75) FROM native_types"
        ).fetchone()[0]
        total = db.execute("SELECT count(*) FROM native_types").fetchone()[0]
        assert expected != total  # the filter must be selective

        compress(db, partition_for(db, "2025-01-15"))

        got = db.execute(
            "SELECT count(*) FILTER (WHERE n > 75) FROM native_types"
        ).fetchone()[0]
        assert got == expected
        # Unfiltered count(*) still works (fast path intact).
        assert db.execute("SELECT count(*) FROM native_types").fetchone()[0] == total
