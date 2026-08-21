"""An unprivileged role must be able to run ordinary DDL in a deltax database.

The ProcessUtility and executor hooks read the deltax catalog through SPI,
which runs as the *current user*. `deltax` is an ordinary schema owned by
whoever ran CREATE EXTENSION, and its default ACL grants nothing to PUBLIC, so
before the install-time grants any role lacking USAGE hit

    ERROR:  permission denied for schema deltax

on the first hook query — turning an unrelated ALTER TABLE, GRANT, or
whole-database ANALYZE into a hard error. Reproduced on PostgreSQL 17 and 18
with a plain NOSUPERUSER role:

    ALTER TABLE           -> ERROR: permission denied for schema deltax
    GRANT                 -> ERROR: permission denied for schema deltax
    ANALYZE (whole-DB)    -> ERROR: permission denied for schema deltax
    ANALYZE <rel>         -> ok  (different branch of the hook)
    VACUUM ANALYZE <rel>  -> ok
    VACUUM / CREATE TABLE -> ok

The role in these tests receives no deltax privileges of its own; it relies
entirely on what CREATE EXTENSION grants to PUBLIC.
"""

import uuid

import psycopg
import pytest

from conftest import HOST_PORT, PG_PASSWORD, PG_USER, _admin_conn

UNPRIVILEGED_PASSWORD = "unprivileged_pw"
MOCK_NOW = "2025-01-15 12:00:00+00"
BASE_TS = "2025-01-15 00:00:00+00"


@pytest.fixture()
def unprivileged(pg_container):
    """Yield (superuser_conn, unprivileged_conn) onto the same deltax-enabled
    database. The second role gets CREATE/USAGE on `public` and nothing else."""
    db_name = "usage_" + uuid.uuid4().hex[:12]
    role_name = "unpriv_" + uuid.uuid4().hex[:8]

    admin = _admin_conn()
    admin.execute(f'CREATE DATABASE "{db_name}"')
    # CREATE ROLE is a utility statement — no bind parameters. The password is
    # a fixed test constant, so inlining it is safe here.
    admin.execute(
        f"CREATE ROLE \"{role_name}\" LOGIN PASSWORD '{UNPRIVILEGED_PASSWORD}'"
    )
    admin.close()

    def _connect(user, password):
        return psycopg.connect(
            host="localhost",
            port=HOST_PORT,
            user=user,
            password=password,
            dbname=db_name,
            autocommit=True,
        )

    su = _connect(PG_USER, PG_PASSWORD)
    su.execute("CREATE EXTENSION pg_deltax")
    su.execute(f'GRANT CREATE, USAGE ON SCHEMA public TO "{role_name}"')
    # Deliberately no explicit grant on deltax — that is the whole point.

    conn = _connect(role_name, UNPRIVILEGED_PASSWORD)
    yield su, conn

    conn.close()
    su.close()
    admin = _admin_conn()
    admin.execute(f'DROP DATABASE "{db_name}"')
    admin.execute(f'DROP ROLE "{role_name}"')
    admin.close()


def _assert_not_superuser(conn):
    """Guard against a test that silently stops testing anything."""
    is_super = conn.execute(
        "SELECT usesuper FROM pg_user WHERE usename = current_user"
    ).fetchone()[0]
    assert is_super is False, "test misconfigured: role must not be a superuser"


def test_ddl_as_unprivileged_role(unprivileged):
    """ALTER TABLE and GRANT both go through the DDL classifier, which looks the
    relation up in deltax_deltatable."""
    _, conn = unprivileged
    _assert_not_superuser(conn)

    conn.execute("CREATE TABLE t (id int, payload text)")
    conn.execute("ALTER TABLE t ADD COLUMN extra int")
    conn.execute("GRANT SELECT ON t TO PUBLIC")

    cols = conn.execute(
        "SELECT count(*) FROM information_schema.columns WHERE table_name = 't'"
    ).fetchone()[0]
    assert cols == 3


def test_vacuum_analyze_as_unprivileged_role(unprivileged):
    """Whole-database ANALYZE reaches the post-VACUUM stats restore, which scans
    deltax_partition. The single-relation forms take a different branch, so
    exercise both."""
    _, conn = unprivileged
    _assert_not_superuser(conn)

    conn.execute("CREATE TABLE t (id int, payload text)")
    conn.execute("INSERT INTO t SELECT g, 'x' FROM generate_series(1, 100) g")

    conn.execute("ANALYZE t")  # explicit-rels branch
    conn.execute("VACUUM ANALYZE t")  # explicit-rels branch
    conn.execute("VACUUM")  # no ANALYZE
    conn.execute("ANALYZE")  # whole-DB (rels = NIL) branch — this one broke


@pytest.mark.xfail(
    strict=True,
    raises=psycopg.errors.InsufficientPrivilege,
    reason="stats restore writes pg_class/pg_statistic as the calling user; see #61",
)
def test_whole_db_analyze_with_a_compressed_partition(unprivileged):
    """Once a partition is compressed, the stats-restore path reads the
    companion `_colstats` tables and then writes `pg_class.reltuples` and
    `pg_statistic` — superuser-only work that no GRANT can reach.

    Both halves are out of reach for an unprivileged caller, and deliberately
    so: companion tables hold user data and the DDL hook mirrors GRANT/REVOKE
    onto them, so widening access there would defeat that mirroring. This is
    therefore a separate bug (#61) with a design decision attached — skip the
    restore for non-superusers, or run it elevated — and is xfail(strict) here
    to document the gap, pin the failure mode so an unrelated breakage can't
    hide beneath it, and flip to a pass the moment #61 is fixed."""
    su, conn = unprivileged
    _assert_not_superuser(conn)

    su.execute(f"SET pg_deltax.mock_now = '{MOCK_NOW}'")
    su.execute(
        "CREATE TABLE metrics ("
        " ts TIMESTAMPTZ NOT NULL, device_id TEXT NOT NULL,"
        " temperature DOUBLE PRECISION)"
    )
    su.execute("SELECT deltax.deltax_create_table('metrics', 'ts', '1 day'::interval)")
    su.execute(
        "INSERT INTO metrics SELECT"
        f" '{BASE_TS}'::timestamptz + (g || ' minutes')::interval,"
        " 'device-' || (g % 10), 20.0 + g * 0.01"
        " FROM generate_series(1, 500) g"
    )

    su.execute(
        "SELECT deltax.deltax_enable_compression('metrics',"
        " segment_by => ARRAY['device_id'], order_by => ARRAY['ts'])"
    )

    # Pick the partition that actually holds the rows — deltax pre-makes
    # empty neighbours, and compressing one of those is a no-op.
    partitions = su.execute(
        "SELECT partition_name FROM deltax.deltax_partition_info('metrics')"
        f" WHERE range_start <= '{BASE_TS}'::timestamptz"
        f" AND range_end > '{BASE_TS}'::timestamptz"
    ).fetchall()
    assert len(partitions) == 1, f"expected exactly one populated partition, got {partitions}"
    part_name = partitions[0][0]
    assert su.execute(f'SELECT count(*) FROM "{part_name}"').fetchone()[0] == 500

    result = su.execute(
        f"SELECT deltax.deltax_compress_partition('{part_name}')"
    ).fetchone()[0]
    assert "Compressed" in result, result
    assert su.execute(
        "SELECT is_compressed FROM deltax.deltax_partition_info('metrics')"
        f" WHERE partition_name = '{part_name}'"
    ).fetchone()[0] is True

    # The unprivileged role now runs a whole-database ANALYZE against a
    # database that has a compressed partition.
    conn.execute("ANALYZE")
    conn.execute("VACUUM ANALYZE")

    # And the data is still readable/correct afterwards.
    assert su.execute("SELECT count(*) FROM metrics").fetchone()[0] == 500


def test_dml_and_queries_as_unprivileged_role(unprivileged):
    """Sanity: ordinary work is unaffected, so a regression here means the
    grants changed more than intended."""
    _, conn = unprivileged
    _assert_not_superuser(conn)

    conn.execute("CREATE TABLE t (id int, payload text)")
    conn.execute("INSERT INTO t SELECT g, 'x' FROM generate_series(1, 10) g")
    conn.execute("UPDATE t SET payload = 'y' WHERE id = 1")
    conn.execute("DELETE FROM t WHERE id = 2")
    assert conn.execute("SELECT count(*) FROM t").fetchone()[0] == 9
    conn.execute("DROP TABLE t")


def test_catalog_is_readable_but_not_writable(unprivileged):
    """The grants are read-only: mutation still goes through deltax.* functions
    whose privileges are unchanged."""
    _, conn = unprivileged
    _assert_not_superuser(conn)

    conn.execute("SELECT count(*) FROM deltax.deltax_deltatable")
    conn.execute("SELECT count(*) FROM deltax.deltax_partition")

    with pytest.raises(psycopg.errors.InsufficientPrivilege):
        conn.execute("DELETE FROM deltax.deltax_deltatable")
