"""End-to-end tests for dual-mode segment files (STORAGE_V2 P1).

With `pg_deltax.blob_storage = 'dual'`, the SPI compress path writes — in
addition to the TOAST-backed `<partition>_blobs` companion table — one
immutable `.dxs` file per partition under `$PGDATA/pg_deltax/<db_oid>/`,
recorded in `deltax.deltax_partition.blob_file`. Reads mmap the file and
serve blob slices from it, silently falling back to the TOAST copy on any
file problem (which is why deleting the file must never change results).

These tests exercise: catalog/file creation, query-result equivalence with
the default `toast` mode, fallback after file deletion, and file removal on
decompression.
"""

import psycopg
import pytest

from conftest import HOST_PORT, PG_PASSWORD, PG_USER

# `pg_deltax.mock_now` pins the partition origin so all our test data
# falls into partitions deltax_create_table actually creates.
MOCK_NOW = "2025-01-15 12:00:00+00"
BASE_TS = "2025-01-15 00:00:00+00"

N_SEGMENTS = 4
SEGMENT_SIZE = 200


def _setup_event_table(conn, blob_storage="dual"):
    """Create + populate + compress an `evt` deltax table.

    Same data shape as test_valbitmap.py: N_SEGMENTS × SEGMENT_SIZE rows in
    one daily partition, low-cardinality `event_type`, high-cardinality
    `payload`. Compression runs with `pg_deltax.blob_storage` set to
    `blob_storage`.
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

    rows = []
    for s in range(N_SEGMENTS):
        for i in range(SEGMENT_SIZE):
            order_id = s * SEGMENT_SIZE + i
            ts = (
                f"'{BASE_TS}'::timestamptz + "
                f"interval '{s * SEGMENT_SIZE + i} seconds'"
            )
            et = "rare" if (s == 0 and i % 25 == 0) else (
                "common" if i % 2 == 0 else "meh"
            )
            payload = f"'p{i}'" if i % 10 else "NULL"
            rows.append(f"({ts}, {order_id}, '{et}', {payload})")

    batch = 500
    for i in range(0, len(rows), batch):
        conn.execute(
            "INSERT INTO evt (ts, order_id, event_type, payload) VALUES "
            + ", ".join(rows[i:i + batch])
        )
    conn.commit()

    conn.execute(
        "SELECT deltax.deltax_enable_compression('evt', "
        f"order_by => ARRAY['order_id'], segment_size => {SEGMENT_SIZE})"
    )
    conn.execute(f"SET pg_deltax.blob_storage = '{blob_storage}'")
    conn.commit()

    parts = conn.execute(
        "SELECT partition_name FROM deltax.deltax_partition_info('evt') "
        "WHERE partition_name NOT LIKE '%default%'"
    ).fetchall()
    for (part_name,) in parts:
        # partition_info can list stale rows from a previously dropped `evt`
        # when this helper runs twice in one test database.
        exists = conn.execute(
            f"SELECT to_regclass('\"{part_name}\"') IS NOT NULL"
        ).fetchone()[0]
        if not exists:
            continue
        n = conn.execute(f'SELECT count(*) FROM "{part_name}"').fetchone()[0]
        if n == 0:
            continue
        conn.execute(f"SELECT deltax.deltax_compress_partition('{part_name}')")
    conn.commit()


# Query mix: point lookup, aggregate with GROUP BY, and a full scan. Each
# decompresses through a different read path (fetch-on-claim, DeltaXAgg,
# DeltaXAppend) so a file-vs-toast divergence anywhere should surface.
QUERIES = [
    "SELECT order_id, event_type, payload FROM {t} WHERE order_id = 137",
    "SELECT event_type, count(*), min(order_id), max(order_id) "
    "FROM {t} GROUP BY event_type ORDER BY event_type",
    "SELECT count(*), count(payload), sum(order_id) FROM {t}",
    "SELECT order_id, event_type, payload FROM {t} ORDER BY order_id",
]


def _run_queries(conn, table="evt"):
    return [conn.execute(q.format(t=table)).fetchall() for q in QUERIES]


def _blob_files(conn, prefix="evt"):
    """(table_name, blob_file) pairs for compressed `prefix*` partitions."""
    return conn.execute(
        "SELECT table_name, blob_file FROM deltax.deltax_partition "
        "WHERE is_compressed AND table_name LIKE %s ORDER BY table_name",
        (prefix + "%",),
    ).fetchall()


def _file_exists(conn, rel_path):
    """Probe a $PGDATA-relative path via pg_stat_file (superuser)."""
    try:
        conn.execute("SELECT pg_stat_file(%s)", (rel_path,)).fetchone()
        return True
    except psycopg.Error:
        conn.rollback()
        return False


def _fresh_conn(conn):
    """Open a new connection (fresh backend → empty mmap cache) to the same DB."""
    dbname = conn.execute("SELECT current_database()").fetchone()[0]
    return psycopg.connect(
        host="localhost",
        port=HOST_PORT,
        user=PG_USER,
        password=PG_PASSWORD,
        dbname=dbname,
    )


def _delete_file(conn, rel_path):
    """Remove a file inside the container's data directory."""
    datadir = conn.execute("SHOW data_directory").fetchone()[0]
    conn.execute(
        f"COPY (SELECT 1) TO PROGRAM 'rm -f {datadir}/{rel_path}'"
    )
    conn.commit()


def _setup_backfill_table(conn, name, blob_storage):
    """Create an empty deltax table and load it via direct backfill
    (COPY ... FORMAT deltax_compress_csv), exercising src/copy.rs.

    Same column shape as `_setup_event_table` so QUERIES apply. Two daily
    partitions (2025-01-14, 2025-01-15) with N_SEGMENTS × SEGMENT_SIZE rows
    each; `order_id` is unique across days so the point lookup stays exact.
    """
    import io

    conn.execute(f"DROP TABLE IF EXISTS {name} CASCADE")
    conn.execute(f"SET pg_deltax.mock_now = '{MOCK_NOW}'")
    conn.execute(f"""
        CREATE TABLE {name} (
            ts          timestamptz NOT NULL,
            order_id    integer NOT NULL,
            event_type  text NOT NULL,
            payload     text
        )
    """)
    conn.execute(
        f"SELECT deltax.deltax_create_table('{name}', 'ts', '1 day'::interval)"
    )
    conn.execute(
        f"SELECT deltax.deltax_enable_compression('{name}', "
        f"order_by => ARRAY['order_id'], segment_size => {SEGMENT_SIZE})"
    )
    conn.execute(f"SET pg_deltax.blob_storage = '{blob_storage}'")
    conn.commit()

    buf = io.StringIO()
    for d, day in enumerate(["2025-01-14", "2025-01-15"]):
        for i in range(N_SEGMENTS * SEGMENT_SIZE):
            order_id = d * 100000 + i
            et = "common" if i % 2 == 0 else "meh"
            payload = f"p{i}" if i % 10 else ""  # unquoted empty → NULL
            buf.write(
                f"{day} 00:{i // 60:02d}:{i % 60:02d}+00,"
                f"{order_id},{et},{payload}\n"
            )
    sql = (
        f"COPY {name} FROM STDIN WITH "
        "(FORMAT deltax_compress_csv, DELIMITER ',')"
    )
    with conn.cursor() as cur:
        with cur.copy(sql) as copy:
            copy.write(buf.getvalue().encode())
    conn.commit()


class TestBlobFile:
    def test_dual_mode_sets_blob_file_and_creates_file(self, db):
        _setup_event_table(db, blob_storage="dual")
        rows = _blob_files(db)
        assert rows, "no compressed partitions"
        for table_name, blob_file in rows:
            assert blob_file, f"blob_file not set for {table_name}"
            assert blob_file.startswith("pg_deltax/"), blob_file
            assert blob_file.endswith(".dxs"), blob_file
            assert _file_exists(db, blob_file), f"missing file {blob_file}"

    def test_toast_mode_leaves_blob_file_null(self, db):
        _setup_event_table(db, blob_storage="toast")
        rows = _blob_files(db)
        assert rows, "no compressed partitions"
        for table_name, blob_file in rows:
            assert blob_file is None, (
                f"blob_file unexpectedly set for {table_name}: {blob_file}"
            )

    def test_dual_results_match_toast_and_raw(self, db):
        """The same queries must return identical results before compression
        (raw heap), compressed in toast mode, and compressed in dual mode
        (served from the segment file)."""
        _setup_event_table(db, blob_storage="toast")
        toast_results = _run_queries(db)

        # Decompress and recompress the same table under dual — avoids
        # drop/recreate (which leaves stale deltax_partition rows in this
        # shared test database) and exercises the cleanup+rewrite path.
        parts = db.execute(
            "SELECT partition_name FROM deltax.deltax_partition_info('evt') "
            "WHERE is_compressed"
        ).fetchall()
        for (p,) in parts:
            db.execute(f"SELECT deltax.deltax_decompress_partition('{p}')")
        db.execute("SET pg_deltax.blob_storage = 'dual'")
        for (p,) in parts:
            db.execute(f"SELECT deltax.deltax_compress_partition('{p}')")
        db.commit()
        assert all(bf for _, bf in _blob_files(db))
        dual_results = _run_queries(db)
        assert dual_results == toast_results

        # A fresh backend reads through the mmap path from scratch.
        with _fresh_conn(db) as fresh:
            assert _run_queries(fresh) == toast_results

    def test_deleting_file_falls_back_to_toast(self, db):
        """Dual mode keeps the TOAST copy authoritative as a fallback:
        removing the .dxs file must not change any query result."""
        _setup_event_table(db, blob_storage="dual")
        expected = _run_queries(db)

        for _, blob_file in _blob_files(db):
            _delete_file(db, blob_file)
            assert not _file_exists(db, blob_file)

        # Fresh backend: the mmap cache is empty, the open fails, and the
        # scan silently falls back to the TOAST blobs table.
        with _fresh_conn(db) as fresh:
            assert _run_queries(fresh) == expected
        # Catalog still references the (gone) file — fallback is per-read.
        assert all(bf for _, bf in _blob_files(db))

    def test_verify_checksums_guc_roundtrip(self, db):
        """With pg_deltax.verify_file_checksums=on every blob is CRC-checked
        on read; results must be unchanged on an intact file."""
        _setup_event_table(db, blob_storage="dual")
        expected = _run_queries(db)
        with _fresh_conn(db) as fresh:
            fresh.execute("SET pg_deltax.verify_file_checksums = on")
            assert _run_queries(fresh) == expected

    def test_decompress_removes_file(self, db):
        _setup_event_table(db, blob_storage="dual")
        files = _blob_files(db)
        assert files and all(bf for _, bf in files)

        total = db.execute("SELECT count(*) FROM evt").fetchone()[0]
        for table_name, _ in files:
            db.execute(
                f"SELECT deltax.deltax_decompress_partition('{table_name}')"
            )
        db.commit()

        for _, blob_file in files:
            assert not _file_exists(db, blob_file), (
                f"segment file {blob_file} not removed by decompress"
            )
        rows = db.execute(
            "SELECT table_name, blob_file FROM deltax.deltax_partition "
            "WHERE table_name = ANY(%s)",
            ([t for t, _ in files],),
        ).fetchall()
        assert all(bf is None for _, bf in rows)
        assert db.execute("SELECT count(*) FROM evt").fetchone()[0] == total

    def test_direct_backfill_dual_writes_blob_file(self, db):
        """COPY ... (FORMAT deltax_compress_csv) exercises src/copy.rs — a
        separate build path that must produce the same dual-mode state as
        the SPI compress path: blob_file set + .dxs file present, identical
        results to toast mode, and file removal on decompress."""
        # Toast baseline: same data through the same COPY path.
        _setup_backfill_table(db, "bf_toast", blob_storage="toast")
        toast_results = _run_queries(db, "bf_toast")
        toast_files = _blob_files(db, "bf_toast")
        assert len(toast_files) == 2, f"expected 2 partitions: {toast_files}"
        assert all(bf is None for _, bf in toast_files)

        # Dual mode: every compressed partition gets a segment file.
        _setup_backfill_table(db, "bf_dual", blob_storage="dual")
        files = _blob_files(db, "bf_dual")
        assert len(files) == 2, f"expected 2 partitions: {files}"
        for table_name, blob_file in files:
            assert blob_file, f"blob_file not set for {table_name}"
            assert blob_file.startswith("pg_deltax/"), blob_file
            assert blob_file.endswith(".dxs"), blob_file
            assert _file_exists(db, blob_file), f"missing file {blob_file}"
            # No leftover .tmp from the incremental writer.
            assert not _file_exists(db, blob_file + ".tmp")

        assert _run_queries(db, "bf_dual") == toast_results
        # A fresh backend reads through the mmap path from scratch.
        with _fresh_conn(db) as fresh:
            assert _run_queries(fresh, "bf_dual") == toast_results

        # Decompress unlinks the file and clears blob_file; data survives.
        for table_name, _ in files:
            db.execute(
                f"SELECT deltax.deltax_decompress_partition('{table_name}')"
            )
        db.commit()
        for _, blob_file in files:
            assert not _file_exists(db, blob_file), (
                f"segment file {blob_file} not removed by decompress"
            )
        rows = db.execute(
            "SELECT table_name, blob_file FROM deltax.deltax_partition "
            "WHERE table_name = ANY(%s)",
            ([t for t, _ in files],),
        ).fetchall()
        assert rows and all(bf is None for _, bf in rows)
        assert _run_queries(db, "bf_dual") == toast_results
