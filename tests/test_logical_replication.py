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


def test_partition_attached_after_publication_auto_publishes(replicated_pair):
    """A partition attached to the parent AFTER the publication is created
    is automatically part of the published stream.

    pg_deltax's worker creates partitions with plain
    `CREATE TABLE … PARTITION OF parent FOR VALUES FROM (…) TO (…)`. We
    simulate that here directly (the worker only runs in the `postgres`
    DB, not in these per-test DBs) and prove the new partition's inserts
    flow through without us touching the publication.
    """
    repl = replicated_pair
    _setup_publication_and_subscription(repl)

    # Sanity: existing partitions replicate.
    with _db_conn(repl["pub_db"]) as pub:
        pub.execute(f"SET pg_deltax.mock_now = '{MOCK_NOW}'")
        pub.execute(
            f"INSERT INTO metrics (ts, device_id, temperature) VALUES "
            f"('{BASE_TS}'::timestamptz, 'd-A', 1.0)"
        )
    with _db_conn(repl["sub_db"]) as sub:
        _wait_for_row_count(sub, "metrics", 1)

    # Attach a brand-new partition on both sides covering a date range
    # well outside what deltax_create_table pre-made — i.e. a partition
    # that did not exist when CREATE PUBLICATION ran. This is exactly the
    # operation pg_deltax's worker performs on its 60s tick.
    far_start = "2025-08-10 00:00:00+00"
    far_end = "2025-08-11 00:00:00+00"
    new_partition = "metrics_p20250810"
    for db in (repl["pub_db"], repl["sub_db"]):
        with _db_conn(db) as c:
            c.execute(
                f"CREATE TABLE {new_partition} PARTITION OF metrics "
                f"FOR VALUES FROM ('{far_start}') TO ('{far_end}')"
            )

    # Insert a row that PG routes into the new partition on the publisher.
    # If publish_via_partition_root is doing its job, this INSERT shows up
    # in the WAL stream and reaches the subscriber's matching new partition
    # — without any ALTER PUBLICATION call in between.
    with _db_conn(repl["pub_db"]) as pub:
        pub.execute(
            f"INSERT INTO metrics (ts, device_id, temperature) VALUES "
            f"('{far_start}'::timestamptz, 'd-FUTURE', 999.0)"
        )
        pub_in_new = pub.execute(
            f"SELECT count(*) FROM ONLY {new_partition}"
        ).fetchone()[0]
        assert pub_in_new == 1, "row didn't route to the new publisher partition"

    with _db_conn(repl["sub_db"]) as sub:
        _wait_for_row_count(sub, "metrics", 2)
        sub_in_new = sub.execute(
            f"SELECT count(*) FROM ONLY {new_partition}"
        ).fetchone()[0]
        assert sub_in_new == 1, (
            "subscriber did not receive INSERT into the newly-attached "
            "partition — publish_via_partition_root failed to auto-include it"
        )
        sub_in_default = sub.execute(
            "SELECT count(*) FROM ONLY metrics_default"
        ).fetchone()[0]
        assert sub_in_default == 0


def test_scenario_2_replicate_companion_tables_directly(replicated_pair):
    """Scenario 2: replicate the COMPANION TABLES themselves so the replica
    never has to run compression — it just receives compressed bytes and uses
    pg_deltax's custom scan for queries.

    Structural caveat with vanilla logical replication
    --------------------------------------------------
    pg_deltax creates companion tables (`_deltax_compressed.<part>_meta`,
    `_blobs`, `_colstats`, `_blooms`, `_text_lengths`, `_valbitmap`) on demand
    when a partition gets compressed. Logical replication does not replicate
    DDL — so a fresh subscriber would receive INSERT events targeting tables
    it has never seen and the apply worker would error.

    For a production "query-only replica" model the right tool is *physical*
    streaming replication: pg_deltax's companion tables are ordinary heaps,
    so the whole storage layout — heap partitions, companion tables, catalog
    rows, even the DML-reject trigger — replicates byte-for-byte to the
    standby, and the worker on the replica no-ops because it checks
    `pg_is_in_recovery()`.

    What this test does
    -------------------
    Demonstrates that the logical-replication *transport* is capable of
    moving companion-table contents and `deltax_partition` catalog rows once
    the schema bootstrap problem is solved. We solve it the brute-force way:
    run identical compression on both sides first (so matching companion
    table shells exist), then wipe the subscriber's companion data + catalog
    row to simulate "received nothing yet", then let a fresh subscription
    with `copy_data = true` stream everything through. A real-world variant
    of (b) above would replace the "compress on both sides" bootstrap with a
    helper that pre-creates companion-table shells on the subscriber.
    """
    repl = replicated_pair

    # 1. Bootstrap: identical setup + compression on both sides. After this
    #    the publisher and subscriber have matching companion-table shapes
    #    and equivalent catalog rows.
    for db in (repl["pub_db"], repl["sub_db"]):
        with _db_conn(db) as c:
            _create_metrics_table(c)
            c.execute(f"SET pg_deltax.mock_now = '{MOCK_NOW}'")
            c.execute(
                f"INSERT INTO metrics (ts, device_id, temperature) "
                f"SELECT '{BASE_TS}'::timestamptz + (g * interval '1 minute'), "
                f"       'device-' || MOD(g, 5)::text, 20.0 + g * 0.1 "
                f"FROM generate_series(0, 99) g"
            )
            c.execute(
                "SELECT deltax_enable_compression('metrics', "
                "segment_by => ARRAY['device_id'], order_by => ARRAY['ts'])"
            )
            cur = c.execute(
                f"SELECT partition_name FROM deltax_partition_info('metrics') "
                f"WHERE range_start <= '{BASE_TS}'::timestamptz "
                f"  AND range_end   >  '{BASE_TS}'::timestamptz"
            )
            parts_to_compress = [r[0] for r in cur.fetchall()]
            for part in parts_to_compress:
                c.execute(f"SELECT deltax_compress_partition('{part}')")
            # Sanity-check that compression produced segments in the meta table.
            seg_count = c.execute(
                f'SELECT count(*) FROM "_deltax_compressed".'
                f'"{parts_to_compress[0]}_meta"'
            ).fetchone()[0]
            assert seg_count > 0, f"[{db}] compression produced no segments"

    # 2. Wipe ONLY the subscriber's companion-table contents. The catalog
    #    row (`deltax_partition`) stays as `is_compressed = true` — that
    #    keeps pg_deltax in compressed-read mode, and (importantly) avoids
    #    a UNIQUE-key conflict during the subscription's initial COPY, since
    #    we won't publish `deltax_partition`.
    with _db_conn(repl["sub_db"]) as sub:
        cur = sub.execute(
            "SELECT format('%I.%I', schemaname, tablename) "
            "FROM pg_tables WHERE schemaname = '_deltax_compressed'"
        )
        for (fqn,) in cur.fetchall():
            sub.execute(f"TRUNCATE TABLE {fqn}")

    # Confirm subscriber's companion tables really are empty before we wire
    # up replication. Note: `SELECT count(*) FROM metrics` would still return
    # 100 here, because pg_deltax has a count(*) pushdown that reads the
    # immutable `deltax_partition.row_count` directly — bypassing the meta
    # scan. We didn't reset that column (intentionally, so we don't have to
    # publish `deltax_partition` and dodge UNIQUE-key conflicts on initial
    # copy). Real-row queries with a WHERE clause go through the meta scan
    # and correctly observe 0.
    with _db_conn(repl["sub_db"]) as sub:
        meta_rows = sub.execute(
            'SELECT count(*) FROM "_deltax_compressed"."metrics_p20250615_meta"'
        ).fetchone()[0]
        assert meta_rows == 0, f"meta table not empty after truncate: {meta_rows}"
        actual_rows = sub.execute(
            f"SELECT count(*) FROM metrics "
            f"WHERE ts >= '{BASE_TS}'::timestamptz"
        ).fetchone()[0]
        assert actual_rows == 0, (
            f"subscriber returned {actual_rows} rows via meta scan after "
            f"truncate — expected 0"
        )

    # 3. Publication scoped to JUST `_deltax_compressed.*` — exactly the
    #    surface area scenario 2 cares about. We omit `metrics` itself
    #    (heap is empty on both sides anyway, and including the parent
    #    partitioned table would require deciding on publish_via_partition_root)
    #    and omit `deltax_partition` to dodge the UNIQUE conflict mentioned
    #    above. In a real "scenario 2" deployment the catalog row would be
    #    kept in sync by some other mechanism — pgrx hooks, a periodic
    #    refresh task, or just physical streaming replication.
    with _db_conn(repl["pub_db"], autocommit=True) as pub:
        pub.execute(
            f"CREATE PUBLICATION {repl['pub_name']} "
            f"FOR TABLES IN SCHEMA _deltax_compressed"
        )
        pub.execute(
            f"SELECT pg_create_logical_replication_slot("
            f"'{repl['pub_slot']}', 'pgoutput')"
        )

    # 4. Subscription with `copy_data = true` — the initial table sync will
    #    refill the subscriber's empty companion tables and re-insert the
    #    `deltax_partition` row, restoring the compressed state from the
    #    publisher's bytes.
    with _db_conn(repl["sub_db"], autocommit=True) as sub:
        sub.execute(
            f"CREATE SUBSCRIPTION {repl['sub_name']} "
            f"CONNECTION '{repl['conn_str']}' "
            f"PUBLICATION {repl['pub_name']} "
            f"WITH (copy_data = true, slot_name = '{repl['pub_slot']}', "
            f"      create_slot = false)"
        )

    # 5. Wait for initial sync to fill the subscriber's companion tables,
    #    then verify a real-row query returns the full set. We use a WHERE
    #    clause so the query goes through the meta scan (DeltaXDecompress),
    #    not the count(*) pushdown that bypasses the meta table.
    with _db_conn(repl["sub_db"]) as sub:
        deadline = time.time() + REPLICATION_TIMEOUT_S
        n = None
        while time.time() < deadline:
            n = sub.execute(
                f"SELECT count(*) FROM metrics "
                f"WHERE ts >= '{BASE_TS}'::timestamptz"
            ).fetchone()[0]
            sub.commit()
            if n == 100:
                break
            time.sleep(0.2)
        assert n == 100, (
            f"subscriber has {n} rows after initial sync, expected 100 — "
            f"companion-table replication may have failed"
        )

        # Belt and braces: the data really came through companion tables,
        # not the leaf-partition heap. The compressed leaf's heap is still
        # empty (compression truncated it on both sides; we never published
        # `metrics` itself).
        cur = sub.execute(
            "SELECT table_name FROM deltax_partition "
            "WHERE is_compressed = true AND table_name LIKE 'metrics_p%'"
        )
        compressed_parts = [r[0] for r in cur.fetchall()]
        assert compressed_parts

        # The companion `_meta` table should now have segment rows again,
        # delivered by the subscription. (Note: `SELECT count(*) FROM ONLY
        # <part>` does NOT bypass the custom scan — pg_deltax still reads
        # through the companion tables for compressed partitions, so it
        # can't be used to inspect the raw heap.)
        meta_segments = sub.execute(
            f'SELECT count(*) FROM "_deltax_compressed"."{compressed_parts[0]}_meta"'
        ).fetchone()[0]
        assert meta_segments > 0, (
            "companion _meta table is still empty after subscription sync — "
            "FOR TABLES IN SCHEMA didn't pick up companion contents"
        )

        # And the actual decompressed values match what we put in. Use a
        # narrow predicate that forces a real meta scan + decompression.
        sample = sub.execute(
            f"SELECT device_id, temperature FROM metrics "
            f"WHERE ts = '{BASE_TS}'::timestamptz + interval '50 minutes'"
        ).fetchall()
        assert len(sample) == 1
        device, temp = sample[0]
        assert device == "device-0"  # MOD(50, 5) = 0
        assert temp == pytest.approx(20.0 + 50 * 0.1)
