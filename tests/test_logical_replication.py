"""Logical replication tests for pg_deltax.

These tests verify that PostgreSQL native logical replication keeps working
when both sides of the stream are pg_deltax-managed tables — i.e. that the
extension does not break replication.

Topology
--------
Publisher and subscriber both live as separate databases inside the single
test container. That keeps the test self-contained (no second container)
and is functionally equivalent to a two-instance setup as far as logical
decoding is concerned. The container is started with `wal_level=logical`
(see conftest.py).

What is verified
----------------
1. INSERTs into a pg_deltax-managed (partitioned) table replicate end-to-end
   when the publication is created with `publish_via_partition_root = true`.
2. UPDATE / DELETE replicate once `REPLICA IDENTITY FULL` is set on the
   parent (pg_deltax tables typically have no primary key, so FULL is the
   simplest way to get full DML replication for time-series workloads that
   need it).
3. Compression on the publisher does not destroy data on the subscriber,
   *provided* the publication excludes TRUNCATE from `publish`. This is
   the one non-obvious thing a pg_deltax user needs to know: the
   compression flow runs `TRUNCATE <partition>` after the row data has
   been copied to the companion tables in `_deltax_compressed.*`, and a
   blindly-replicated TRUNCATE would wipe rows on the subscriber that
   the subscriber has not yet compressed.
"""

import contextlib
import time
import uuid

import psycopg
import pytest
from conftest import HOST_PORT, PG_PASSWORD, PG_USER

# Inside the container, postgres always listens on localhost:5432 — this is
# the connection string the subscriber's wal_receiver uses to reach the
# publisher.
SUBSCRIBER_CONN_TO_PUBLISHER = (
    f"host=localhost port=5432 user={PG_USER} password={PG_PASSWORD} dbname={{db}}"
)

# Pin "now" so partitions cover the test timestamps deterministically.
MOCK_NOW = "2025-06-15 12:00:00+00"
BASE_TS = "2025-06-15 00:00:00+00"

REPLICATION_TIMEOUT_S = 30


@contextlib.contextmanager
def _db_conn(db_name, autocommit=False):
    """Yield a connection that is guaranteed to be closed on exit.

    We need this because psycopg3's own context manager only handles the
    transaction, not the socket — and a lingering backend with an open
    transaction will block logical-replication slot creation indefinitely.
    """
    conn = psycopg.connect(
        host="localhost",
        port=HOST_PORT,
        user=PG_USER,
        password=PG_PASSWORD,
        dbname=db_name,
        autocommit=autocommit,
    )
    try:
        yield conn
        if not autocommit:
            conn.commit()
    except Exception:
        if not autocommit:
            conn.rollback()
        raise
    finally:
        conn.close()


def _admin_conn_cm():
    return _db_conn("postgres", autocommit=True)


def _create_metrics_table(conn):
    """Create a pg_deltax-managed table with the same shape on either side."""
    conn.execute(f"SET pg_deltax.mock_now = '{MOCK_NOW}'")
    conn.execute(
        """
        CREATE TABLE metrics (
            ts TIMESTAMPTZ NOT NULL,
            device_id TEXT NOT NULL,
            temperature DOUBLE PRECISION
        )
        """
    )
    conn.execute("SELECT deltax_create_table('metrics', 'ts', '1 day'::interval)")
    conn.commit()


def _set_replica_identity_on_all_leaves(conn, table, identity):
    """Set REPLICA IDENTITY on the parent AND every existing leaf partition.

    Postgres applies the REPLICA IDENTITY check at the leaf-partition storage
    level — setting it on the parent only is silently ignored for UPDATE/DELETE
    on the leaves. This helper covers everything that exists right now; note
    that pg_deltax's worker creates new partitions on the fly, so a production
    deployment that needs UPDATE/DELETE replication must also set REPLICA
    IDENTITY on those as they appear (or use a primary key / unique index
    that propagates with the partition definition).
    """
    conn.execute(f"ALTER TABLE {table} REPLICA IDENTITY {identity}")
    cur = conn.execute(
        f"""
        SELECT quote_ident(n.nspname) || '.' || quote_ident(c.relname)
        FROM pg_class p
        JOIN pg_inherits i ON i.inhparent = p.oid
        JOIN pg_class c ON c.oid = i.inhrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE p.relname = '{table}'
        """
    )
    for (qualified,) in cur.fetchall():
        conn.execute(f"ALTER TABLE {qualified} REPLICA IDENTITY {identity}")


def _wait_for_row_count(conn, table, expected, timeout_s=REPLICATION_TIMEOUT_S):
    """Poll until `table` has `expected` rows or timeout. Commits between reads
    so the wal_receiver's snapshot advances on the subscriber."""
    deadline = time.time() + timeout_s
    last = None
    while time.time() < deadline:
        last = conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
        conn.commit()
        if last == expected:
            return last
        time.sleep(0.2)
    raise AssertionError(
        f"replication timeout: {table} has {last} rows, expected {expected}"
    )


@pytest.fixture()
def replicated_pair(pg_container):
    """Provision a publisher_db / subscriber_db pair, drop everything on teardown.

    Each scenario (truncate-on / truncate-off, partition_root on / off, …)
    needs slightly different publication/subscription settings, so the
    fixture just supplies cleanly-named DBs + connection strings and lets
    the test do the wiring.
    """
    suffix = uuid.uuid4().hex[:8]
    pub_db = f"replpub_{suffix}"
    sub_db = f"replsub_{suffix}"
    pub_slot = f"slot_{suffix}"
    sub_name = f"sub_{suffix}"
    pub_name = f"pub_{suffix}"

    with _admin_conn_cm() as admin:
        admin.execute(f'CREATE DATABASE "{pub_db}"')
        admin.execute(f'CREATE DATABASE "{sub_db}"')

    # Bootstrap extension on both sides — each in its own short-lived
    # connection so no zombie transactions remain.
    for db in (pub_db, sub_db):
        with _db_conn(db) as c:
            c.execute("CREATE EXTENSION pg_deltax")

    try:
        yield {
            "pub_db": pub_db,
            "sub_db": sub_db,
            "pub_name": pub_name,
            "sub_name": sub_name,
            "pub_slot": pub_slot,
            "conn_str": SUBSCRIBER_CONN_TO_PUBLISHER.format(db=pub_db),
        }
    finally:
        # Order matters: disable + drop subscription FIRST (drops its slot
        # on the publisher); only then can we drop the databases.
        with _admin_conn_cm() as admin:
            admin.execute(
                f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                f"WHERE datname IN ('{pub_db}', '{sub_db}') "
                f"  AND pid <> pg_backend_pid()"
            )

        try:
            with _db_conn(sub_db, autocommit=True) as c:
                c.execute(f"ALTER SUBSCRIPTION {sub_name} DISABLE")
                c.execute(f"ALTER SUBSCRIPTION {sub_name} SET (slot_name = NONE)")
                c.execute(f"DROP SUBSCRIPTION {sub_name}")
        except psycopg.Error:
            pass

        with _admin_conn_cm() as admin:
            try:
                admin.execute(
                    f"SELECT pg_drop_replication_slot('{pub_slot}') "
                    f"WHERE EXISTS (SELECT 1 FROM pg_replication_slots "
                    f"              WHERE slot_name = '{pub_slot}')"
                )
            except psycopg.Error:
                pass
            admin.execute(f'DROP DATABASE IF EXISTS "{sub_db}" WITH (FORCE)')
            admin.execute(f'DROP DATABASE IF EXISTS "{pub_db}" WITH (FORCE)')


def _setup_publication_and_subscription(
    repl, *, publish="insert, update, delete, truncate", replica_identity=None
):
    """Create matching tables on both sides, then wire pub→sub.

    `publish` controls which operations are replicated. The compression test
    passes `publish='insert, update, delete'` to suppress TRUNCATE — the
    compression flow truncates the partition heap and we don't want that
    truncation to follow the wire.
    """
    pub_db = repl["pub_db"]
    sub_db = repl["sub_db"]

    with _db_conn(pub_db) as pub:
        _create_metrics_table(pub)
        if replica_identity:
            _set_replica_identity_on_all_leaves(pub, "metrics", replica_identity)
    with _db_conn(sub_db) as sub:
        _create_metrics_table(sub)
        if replica_identity:
            _set_replica_identity_on_all_leaves(sub, "metrics", replica_identity)

    # Publication. publish_via_partition_root=true makes the subscriber see
    # changes as if applied to the root, so its own partition routing kicks
    # in independently — no partition-name coupling between the two sides.
    with _db_conn(pub_db, autocommit=True) as pub:
        pub.execute(
            f"CREATE PUBLICATION {repl['pub_name']} "
            f"FOR TABLE metrics "
            f"WITH (publish = '{publish}', publish_via_partition_root = true)"
        )
        # Pre-create the replication slot in its own transaction. We must do
        # this BEFORE issuing CREATE SUBSCRIPTION because publisher and
        # subscriber live in the same Postgres cluster: if CREATE SUBSCRIPTION
        # tries to create the slot, the slot's "wait for older xacts to end"
        # phase will wait on the CREATE SUBSCRIPTION xact itself, deadlocking
        # the test. Creating the slot in a separate short-lived txn avoids it.
        pub.execute(
            f"SELECT pg_create_logical_replication_slot('{repl['pub_slot']}', "
            f"'pgoutput')"
        )

    # Subscription. copy_data=false so we start from an empty snapshot —
    # we want to observe the WAL stream, not the initial table copy.
    # create_slot=false because we already made the slot above.
    with _db_conn(sub_db, autocommit=True) as sub:
        sub.execute(
            f"CREATE SUBSCRIPTION {repl['sub_name']} "
            f"CONNECTION '{repl['conn_str']}' "
            f"PUBLICATION {repl['pub_name']} "
            f"WITH (copy_data = false, slot_name = '{repl['pub_slot']}', "
            f"      create_slot = false)"
        )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_inserts_replicate(replicated_pair):
    """Plain INSERTs into a pg_deltax-managed table replicate end-to-end."""
    repl = replicated_pair
    _setup_publication_and_subscription(repl)

    with _db_conn(repl["pub_db"]) as pub:
        pub.execute(f"SET pg_deltax.mock_now = '{MOCK_NOW}'")
        pub.execute(
            f"INSERT INTO metrics (ts, device_id, temperature) "
            f"SELECT '{BASE_TS}'::timestamptz + (g * interval '1 minute'), "
            f"       'device-' || MOD(g, 5)::text, 20.0 + g * 0.1 "
            f"FROM generate_series(0, 99) g"
        )

    with _db_conn(repl["sub_db"]) as sub:
        _wait_for_row_count(sub, "metrics", 100)
        distinct_devices = sub.execute(
            "SELECT count(DISTINCT device_id) FROM metrics"
        ).fetchone()[0]
        assert distinct_devices == 5

        # All inserted timestamps fall inside the pre-made partitions around
        # mock_now, so nothing should land in the subscriber's default
        # partition — i.e. partition routing fired correctly on the sub.
        in_default = sub.execute(
            "SELECT count(*) FROM ONLY metrics_default"
        ).fetchone()[0]
        assert in_default == 0, "rows leaked into the subscriber's default partition"


def test_updates_and_deletes_replicate(replicated_pair):
    """With REPLICA IDENTITY FULL the publisher's UPDATE/DELETE replicate too.

    pg_deltax tables generally have no primary key (time-series append flows
    don't need one), so the user has to opt in to UPDATE/DELETE replication
    via REPLICA IDENTITY FULL or by adding a unique index. This test is the
    documented escape hatch.
    """
    repl = replicated_pair
    _setup_publication_and_subscription(repl, replica_identity="FULL")

    with _db_conn(repl["pub_db"]) as pub:
        pub.execute(f"SET pg_deltax.mock_now = '{MOCK_NOW}'")
        pub.execute(
            f"INSERT INTO metrics (ts, device_id, temperature) VALUES "
            f"('{BASE_TS}'::timestamptz, 'd-A', 1.0), "
            f"('{BASE_TS}'::timestamptz, 'd-B', 2.0), "
            f"('{BASE_TS}'::timestamptz, 'd-C', 3.0)"
        )

    with _db_conn(repl["sub_db"]) as sub:
        _wait_for_row_count(sub, "metrics", 3)

    with _db_conn(repl["pub_db"]) as pub:
        pub.execute("UPDATE metrics SET temperature = 99.9 WHERE device_id = 'd-A'")
        pub.execute("DELETE FROM metrics WHERE device_id = 'd-C'")

    with _db_conn(repl["sub_db"]) as sub:
        _wait_for_row_count(sub, "metrics", 2)
        temp_a = sub.execute(
            "SELECT temperature FROM metrics WHERE device_id = 'd-A'"
        ).fetchone()[0]
        assert temp_a == pytest.approx(99.9)


def test_compression_with_truncate_excluded_preserves_subscriber(replicated_pair):
    """Compressing a partition on the publisher must not wipe rows on the subscriber.

    pg_deltax's compression flow does `TRUNCATE <partition>` after copying the
    rows into the `_deltax_compressed.*` companion tables — the heap is empty
    while the data lives in the companion side. With `publish='insert,update,
    delete'` (TRUNCATE excluded) the subscriber's heap rows survive, and the
    subscriber can compress independently on its own schedule.

    Without this exclusion the TRUNCATE would replicate and the subscriber's
    data would be destroyed — that is the gotcha a pg_deltax user needs to
    be aware of when designing a logical-replication topology.
    """
    repl = replicated_pair
    _setup_publication_and_subscription(repl, publish="insert, update, delete")

    with _db_conn(repl["pub_db"]) as pub:
        pub.execute(f"SET pg_deltax.mock_now = '{MOCK_NOW}'")
        pub.execute(
            f"INSERT INTO metrics (ts, device_id, temperature) "
            f"SELECT '{BASE_TS}'::timestamptz + (g * interval '1 minute'), "
            f"       'device-' || MOD(g, 5)::text, 20.0 + g * 0.1 "
            f"FROM generate_series(0, 199) g"
        )

    with _db_conn(repl["sub_db"]) as sub:
        _wait_for_row_count(sub, "metrics", 200)

    # Compress on the publisher. The published TRUNCATE event is filtered out
    # by the publication's `publish` list, so the subscriber should retain
    # its heap rows.
    with _db_conn(repl["pub_db"]) as pub:
        pub.execute(
            "SELECT deltax_enable_compression('metrics', "
            "segment_by => ARRAY['device_id'], order_by => ARRAY['ts'])"
        )
        partitions = pub.execute(
            "SELECT partition_name FROM deltax_partition_info('metrics') "
            f"WHERE range_start <= '{BASE_TS}'::timestamptz "
            f"  AND range_end   >  '{BASE_TS}'::timestamptz"
        ).fetchall()
        assert partitions, "expected a partition covering BASE_TS"
        for (part,) in partitions:
            pub.execute(f"SELECT deltax_compress_partition('{part}')")

        # Publisher: the heap partition is empty (data moved to companion
        # tables), but a `SELECT * FROM metrics` still returns 200 rows
        # because the custom scan transparently decompresses.
        pub_total = pub.execute("SELECT count(*) FROM metrics").fetchone()[0]
        assert pub_total == 200

    # Give the apply worker time to process any in-flight events, then
    # assert the subscriber still has all 200 rows in its own heap.
    time.sleep(2)
    with _db_conn(repl["sub_db"]) as sub:
        sub_total = sub.execute("SELECT count(*) FROM metrics").fetchone()[0]
        assert sub_total == 200, (
            "subscriber lost rows after publisher-side compression — "
            "did the publication accidentally include TRUNCATE?"
        )

    # And new inserts into a DIFFERENT (uncompressed) partition still
    # replicate normally. We move forward one day so the row lands in a
    # neighbouring pre-made partition rather than the now-compressed one
    # (pg_deltax rejects DML on compressed partitions — that is by design;
    # the time-series append pattern is to write to the current open
    # partition while older ones get compressed in the background).
    with _db_conn(repl["pub_db"]) as pub:
        pub.execute(
            f"INSERT INTO metrics (ts, device_id, temperature) VALUES "
            f"('{BASE_TS}'::timestamptz + interval '1 day', 'device-NEW', 42.0)"
        )

    with _db_conn(repl["sub_db"]) as sub:
        _wait_for_row_count(sub, "metrics", 201)
