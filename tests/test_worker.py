"""Integration tests for the background worker using pg_deltax.mock_now."""

import time
import uuid

import psycopg

# ALTER SYSTEM requires autocommit (cannot run inside a transaction block),
# so we use a short-lived connection for those statements.
CONN_PARAMS = dict(
    host="localhost", port=15432, user="postgres",
    password="postgres", dbname="postgres",
)


def _alter_system(sql):
    """Run an ALTER SYSTEM statement via a temporary autocommit connection."""
    with psycopg.connect(**CONN_PARAMS, autocommit=True) as conn:
        conn.execute(sql)
        conn.execute("SELECT pg_reload_conf()")


def _unique_table():
    return "wt_" + uuid.uuid4().hex[:8]


def _cleanup(db, table_name):
    """Drop test table and its catalog entries."""
    # Reset the worker's clock
    _alter_system("ALTER SYSTEM RESET pg_deltax.mock_now")

    # The connection may be in an error state; roll back first
    db.rollback()
    db.execute("RESET pg_deltax.mock_now")
    db.execute(
        "DELETE FROM deltax.deltax_partition WHERE deltatable_id IN "
        "(SELECT id FROM deltax.deltax_deltatable WHERE table_name = %s)",
        (table_name,),
    )
    db.execute(
        "DELETE FROM deltax.deltax_deltatable WHERE table_name = %s", (table_name,)
    )
    db.execute(f'DROP TABLE IF EXISTS "{table_name}" CASCADE')
    db.commit()


def test_worker_creates_future_partitions(postgres_db):
    """After time advances past the pre-made window, the worker creates new partitions."""
    db = postgres_db
    table = _unique_table()

    try:
        # Pin "now" to a known time for deterministic partition layout
        db.execute("SET pg_deltax.mock_now = '2025-06-15 00:00:00+00'")
        db.execute(
            f'CREATE TABLE "{table}" (ts TIMESTAMPTZ NOT NULL, val FLOAT8)'
        )
        db.commit()

        # premake=2 → 4 partitions: 1 past + 1 current + 2 future
        #   [June 14-15), [June 15-16), [June 16-17), [June 17-18)
        db.execute(
            f"SELECT deltax.deltax_create_table('{table}', 'ts', '1 day', 2)"
        )
        db.commit()

        initial = db.execute(
            f"SELECT count(*) FROM deltax.deltax_partition_info('{table}')"
        ).fetchone()[0]
        assert initial == 4

        # Jump time forward 5 days. The worker (premake=3) will see that
        # it needs partitions around June 20 and create them.
        _alter_system(
            "ALTER SYSTEM SET pg_deltax.mock_now = '2025-06-20 00:00:00+00'"
        )

        # Poll until the worker has created new partitions (runs every 60s).
        # Commit after each read to release locks — the worker needs
        # ACCESS EXCLUSIVE to create partitions.
        deadline = time.time() + 90
        new_count = initial
        while time.time() < deadline:
            time.sleep(5)
            new_count = db.execute(
                f"SELECT count(*) FROM deltax.deltax_partition_info('{table}')"
            ).fetchone()[0]
            db.commit()
            if new_count > initial:
                break

        assert new_count > initial, (
            f"Expected worker to create new partitions (was {initial}, "
            f"still {new_count} after waiting)"
        )

    finally:
        _cleanup(db, table)


def test_worker_drains_default_partition(postgres_db):
    """Rows landing in the default partition are moved by the worker."""
    db = postgres_db
    table = _unique_table()

    try:
        # Pin "now" so we get predictable partitions
        db.execute("SET pg_deltax.mock_now = '2025-06-15 00:00:00+00'")
        db.execute(
            f'CREATE TABLE "{table}" (ts TIMESTAMPTZ NOT NULL, val FLOAT8)'
        )
        db.commit()

        # premake=1 → 3 partitions: [June 14-15), [June 15-16), [June 16-17)
        db.execute(
            f"SELECT deltax.deltax_create_table('{table}', 'ts', '1 day', 1)"
        )
        db.commit()

        # Insert a row far in the future — lands in the default partition
        db.execute(
            f"INSERT INTO \"{table}\" VALUES ('2025-07-01 12:00:00+00', 99.0)"
        )
        db.commit()

        default_count = db.execute(
            f'SELECT count(*) FROM "{table}_default"'
        ).fetchone()[0]
        assert default_count == 1, "Row should be in the default partition"

        # Tell the worker it's July 1 so it can create a matching partition
        # and drain the default
        _alter_system(
            "ALTER SYSTEM SET pg_deltax.mock_now = '2025-07-01 00:00:00+00'"
        )

        # Poll until the default partition is empty.
        # Commit after each read to release locks — the worker needs
        # ACCESS EXCLUSIVE on the partition to detach/reattach it.
        deadline = time.time() + 90
        drained = False
        while time.time() < deadline:
            time.sleep(5)
            cnt = db.execute(
                f'SELECT count(*) FROM "{table}_default"'
            ).fetchone()[0]
            db.commit()
            if cnt == 0:
                drained = True
                break

        assert drained, "Expected worker to drain the default partition"

        # The row should still be queryable from the parent table
        total = db.execute(f'SELECT count(*) FROM "{table}"').fetchone()[0]
        assert total == 1

    finally:
        _cleanup(db, table)


def test_run_maintenance_drains_synchronously(postgres_db):
    """deltax_run_maintenance() runs the same drain/premake/compress/retention
    pass as the background worker, but synchronously in the calling session.

    This is the entry point session mode schedules externally (e.g. pg_cron).
    The test drives it directly (rather than waiting for the 60s worker), which
    exercises the shared maintenance path including the per-table subtransaction
    wrapper. Because a maintenance pass takes a transaction-level advisory lock,
    a given call can lose the race to the always-running full-mode worker, so we
    call it in a short loop until the default is drained — whichever pass wins,
    the default converges to empty quickly.

    The worker's clock is aligned via ALTER SYSTEM so its concurrent passes stay
    bounded and consistent with the session's view (the same pattern the other
    worker tests use)."""
    db = postgres_db
    table = _unique_table()

    try:
        # Keep the session and the background worker on the same clock so the
        # worker's concurrent maintenance stays bounded.
        _alter_system("ALTER SYSTEM SET pg_deltax.mock_now = '2025-07-01 00:00:00+00'")
        db.execute("SET pg_deltax.mock_now = '2025-06-15 00:00:00+00'")
        db.execute(
            f'CREATE TABLE "{table}" (ts TIMESTAMPTZ NOT NULL, val FLOAT8)'
        )
        db.commit()

        # premake=1 → 3 partitions: [June 14-15), [June 15-16), [June 16-17)
        db.execute(
            f"SELECT deltax.deltax_create_table('{table}', 'ts', '1 day', 1)"
        )
        db.commit()

        # Insert a row far in the future — lands in the default partition.
        db.execute(
            f"INSERT INTO \"{table}\" VALUES ('2025-07-01 12:00:00+00', 99.0)"
        )
        db.commit()

        assert db.execute(
            f'SELECT count(*) FROM "{table}_default"'
        ).fetchone()[0] == 1, "Row should be in the default partition"

        # Advance the session clock so the synchronous pass can create a July 1
        # partition, then drive maintenance ourselves until the default drains.
        db.execute("SET pg_deltax.mock_now = '2025-07-01 00:00:00+00'")
        deadline = time.time() + 30
        drained = False
        while time.time() < deadline:
            db.execute("SELECT deltax.deltax_run_maintenance()")
            db.commit()
            if db.execute(
                f'SELECT count(*) FROM "{table}_default"'
            ).fetchone()[0] == 0:
                drained = True
                break
            time.sleep(2)
            db.commit()

        assert drained, "Expected deltax_run_maintenance() to drain the default"

        # Row still visible from the parent, and a further call is a clean no-op.
        assert db.execute(f'SELECT count(*) FROM "{table}"').fetchone()[0] == 1
        db.execute("SELECT deltax.deltax_run_maintenance()")
        db.commit()
        assert db.execute(f'SELECT count(*) FROM "{table}"').fetchone()[0] == 1

    finally:
        _cleanup(db, table)


def test_worker_retention_drops_old_partitions(postgres_db):
    """Retention policy causes the worker to drop partitions older than drop_after."""
    db = postgres_db
    table = _unique_table()

    try:
        # Pin "now" to a known time
        db.execute("SET pg_deltax.mock_now = '2025-06-15 00:00:00+00'")
        db.execute(
            f'CREATE TABLE "{table}" (ts TIMESTAMPTZ NOT NULL, val FLOAT8)'
        )
        db.commit()

        # premake=2 → 4 partitions:
        #   [June 14-15), [June 15-16), [June 16-17), [June 17-18)
        db.execute(
            f"SELECT deltax.deltax_create_table('{table}', 'ts', '1 day', 2)"
        )
        db.commit()

        # Insert data into the earliest partition (June 14)
        db.execute(
            f"INSERT INTO \"{table}\" VALUES ('2025-06-14 12:00:00+00', 1.0)"
        )
        # Insert data into current partition (June 15)
        db.execute(
            f"INSERT INTO \"{table}\" VALUES ('2025-06-15 12:00:00+00', 2.0)"
        )
        db.commit()

        # Record the original partition names
        initial_partitions = {
            row[0]
            for row in db.execute(
                f"SELECT partition_name FROM deltax.deltax_partition_info('{table}')"
            ).fetchall()
        }
        assert len(initial_partitions) == 4

        # Set retention policy: drop partitions older than 3 days
        db.execute(
            f"SELECT deltax.deltax_set_retention('{table}', '3 days')"
        )
        db.commit()

        # Jump time forward to June 20. Now June 14 partition (range_end June 15)
        # is 5 days old, which exceeds the 3-day retention → should be dropped.
        # June 15 partition (range_end June 16) is 4 days old → also dropped.
        # June 16 partition (range_end June 17) is 3 days old → exactly at boundary, kept.
        # Note: the worker also creates new future partitions, so we check by
        # partition name rather than total count.
        _alter_system(
            "ALTER SYSTEM SET pg_deltax.mock_now = '2025-06-20 00:00:00+00'"
        )

        # Poll until the worker drops the old partitions. The worker wakes
        # every 60s and the mock_now change only takes effect via config
        # reload, so worst case the drop lands on the *second* cycle after
        # the ALTER SYSTEM — a 90s deadline missed that intermittently in CI.
        deadline = time.time() + 150
        dropped = False
        while time.time() < deadline:
            time.sleep(5)
            current_partitions = {
                row[0]
                for row in db.execute(
                    f"SELECT partition_name FROM deltax.deltax_partition_info('{table}')"
                ).fetchall()
            }
            db.commit()
            # Check that at least some original partitions are gone
            if not initial_partitions.issubset(current_partitions):
                dropped = True
                break

        assert dropped, (
            f"Expected worker to drop old partitions, but original partitions "
            f"are still present: {initial_partitions}"
        )

        # Verify the old data is gone — the partitions that held it were dropped
        old_rows = db.execute(
            f"SELECT count(*) FROM \"{table}\" WHERE ts < '2025-06-16 00:00:00+00'"
        ).fetchone()[0]
        assert old_rows == 0, "Old data should have been dropped"

    finally:
        _cleanup(db, table)


def test_worker_retention_drops_compressed_companions(postgres_db):
    """When retention drops a COMPRESSED partition, every companion table in
    _deltax_compressed (meta, colstats, blobs, blooms, text_lengths,
    valbitmap) must be dropped with it — no orphans left behind."""
    db = postgres_db
    table = _unique_table()

    try:
        db.execute("SET pg_deltax.mock_now = '2025-06-15 00:00:00+00'")
        db.execute(
            f'CREATE TABLE "{table}" '
            "(ts TIMESTAMPTZ NOT NULL, event_type TEXT NOT NULL, val FLOAT8)"
        )
        db.commit()
        db.execute(
            f"SELECT deltax.deltax_create_table('{table}', 'ts', '1 day', 2)"
        )
        db.commit()

        # Fill the June 14 partition. The low-cardinality text column makes
        # compression create a `_valbitmap` companion; the text column also
        # produces `_text_lengths`.
        values = ", ".join(
            f"('2025-06-14 12:00:00+00'::timestamptz + interval '{i} seconds', "
            f"'type{i % 4}', {i})"
            for i in range(100)
        )
        db.execute(
            f'INSERT INTO "{table}" (ts, event_type, val) VALUES {values}'
        )
        db.commit()

        db.execute(
            f"SELECT deltax.deltax_enable_compression('{table}', "
            "order_by => ARRAY['ts'])"
        )
        # The June 14 partition is the only one with rows — find its name
        # rather than hardcoding it (partition names render range_start in
        # the session timezone).
        old_part = None
        parts = db.execute(
            f"SELECT partition_name FROM deltax.deltax_partition_info('{table}') "
            "WHERE partition_name NOT LIKE '%default%'"
        ).fetchall()
        for (p,) in parts:
            if db.execute(f'SELECT count(*) FROM "{p}"').fetchone()[0]:
                old_part = p
                break
        assert old_part is not None, f"no non-empty partition among {parts}"
        db.execute(f"SELECT deltax.deltax_compress_partition('{old_part}')")
        db.commit()

        def list_companions():
            return [
                r[0]
                for r in db.execute(
                    "SELECT c.relname FROM pg_class c "
                    "JOIN pg_namespace n ON n.oid = c.relnamespace "
                    "WHERE n.nspname = '_deltax_compressed' "
                    "AND c.relname LIKE %s AND c.relkind = 'r'",
                    (old_part + r"\_%",),
                ).fetchall()
            ]

        before = set(list_companions())
        assert f"{old_part}_meta" in before, f"companions missing: {before}"
        assert f"{old_part}_valbitmap" in before, (
            f"expected a _valbitmap companion for the low-card text column, "
            f"got: {before}"
        )

        db.execute(f"SELECT deltax.deltax_set_retention('{table}', '3 days')")
        db.commit()

        # Jump time so the June 14 partition exceeds the retention window.
        _alter_system(
            "ALTER SYSTEM SET pg_deltax.mock_now = '2025-06-20 00:00:00+00'"
        )

        deadline = time.time() + 150
        dropped = False
        while time.time() < deadline:
            time.sleep(5)
            still = {
                row[0]
                for row in db.execute(
                    f"SELECT partition_name FROM deltax.deltax_partition_info('{table}')"
                ).fetchall()
            }
            db.commit()
            if old_part not in still:
                dropped = True
                break

        assert dropped, f"worker did not drop partition {old_part}"

        leftovers = list_companions()
        db.commit()
        assert leftovers == [], (
            f"orphaned companion tables left in _deltax_compressed after "
            f"retention drop: {leftovers}"
        )

    finally:
        _cleanup(db, table)
