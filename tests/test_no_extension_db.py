"""Regression test for issue #24.

When `pg_deltax` is in `shared_preload_libraries` (cluster-wide, as required
for the background worker), its ProcessUtility hook fires in *every* database
in the cluster — including databases where `CREATE EXTENSION pg_deltax` was
never run. In such a database the hook must no-op and let the statement
through; it must NOT try to query `deltax.deltax_deltatable`, which only
exists where the extension was actually created.

The original bug: `ALTER TABLE` in a no-extension database hard-failed with

    ERROR:  relation "deltax.deltax_deltatable" does not exist

while `CREATE TABLE` / `CREATE INDEX` / `INSERT` / `DROP TABLE` were fine.
"""

import uuid

import psycopg
import pytest

from conftest import HOST_PORT, PG_PASSWORD, PG_USER, _admin_conn


@pytest.fixture()
def db_without_extension(pg_container):
    """A fresh database that does NOT have `CREATE EXTENSION pg_deltax`.

    The cluster still has `pg_deltax` in `shared_preload_libraries` (set up by
    the session-scoped `pg_container` fixture), so the ProcessUtility hook is
    active even though the extension's catalog is absent here.
    """
    db_name = "noext_" + uuid.uuid4().hex[:12]

    admin = _admin_conn()
    admin.execute(f'CREATE DATABASE "{db_name}"')
    admin.close()

    conn = psycopg.connect(
        host="localhost",
        port=HOST_PORT,
        user=PG_USER,
        password=PG_PASSWORD,
        dbname=db_name,
    )
    # Deliberately do NOT run CREATE EXTENSION pg_deltax.
    yield conn

    conn.close()
    admin = _admin_conn()
    admin.execute(f'DROP DATABASE "{db_name}"')
    admin.close()


def test_alter_table_in_database_without_extension(db_without_extension):
    """The full reproduction from issue #24: every DDL statement, including
    `ALTER TABLE`, must succeed in a database without the extension."""
    conn = db_without_extension

    # Sanity: the extension's catalog really is absent in this database.
    schema_present = conn.execute(
        "SELECT to_regclass('deltax.deltax_deltatable') IS NOT NULL"
    ).fetchone()[0]
    assert schema_present is False, (
        "test misconfigured: deltax catalog should not exist in this database"
    )

    # These all worked even with the bug present.
    conn.execute("CREATE TABLE t (id int)")
    conn.execute("CREATE INDEX t_idx ON t (id)")
    conn.execute("INSERT INTO t VALUES (1)")
    conn.execute("DROP TABLE t")

    # This is what broke: ALTER TABLE went through the ProcessUtility hook,
    # which queried the (missing) deltax catalog and raised
    # `relation "deltax.deltax_deltatable" does not exist`.
    conn.execute("CREATE TABLE u (id int)")
    conn.execute("ALTER TABLE u ADD COLUMN y int")
    conn.commit()

    cols = conn.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name = 'u' ORDER BY ordinal_position"
    ).fetchall()
    assert [c[0] for c in cols] == ["id", "y"]


def test_all_ddl_classifier_branches_without_extension(db_without_extension):
    """Defense in depth: the ProcessUtility hook routes more than just
    `ALTER TABLE`. Every statement the hook classifies — RENAME (table,
    column, index), `SET SCHEMA`, and GRANT/REVOKE — went through the same
    catalog lookup that broke in issue #24 and must also no-op in a
    no-extension database.

    Each line below exercises a distinct classifier in `src/ddl.rs`:
      handle_alter_table        — AlterTableStmt
      handle_rename             — RenameStmt (table / column / index)
      handle_alter_object_schema — AlterObjectSchemaStmt (SET SCHEMA)
      handle_grant              — GrantStmt (GRANT / REVOKE)
    """
    conn = db_without_extension
    conn.execute("CREATE SCHEMA other_schema")
    conn.execute("CREATE TABLE base (id int, name text)")
    conn.execute("CREATE INDEX base_idx ON base (id)")

    # handle_alter_table
    conn.execute("ALTER TABLE base ADD COLUMN extra int")
    conn.execute("ALTER TABLE base ALTER COLUMN name SET NOT NULL")
    conn.execute("ALTER TABLE base DROP COLUMN extra")

    # handle_rename — column, index, then the table itself
    conn.execute("ALTER TABLE base RENAME COLUMN name TO label")
    conn.execute("ALTER INDEX base_idx RENAME TO base_idx2")
    conn.execute("ALTER TABLE base RENAME TO renamed")

    # handle_alter_object_schema
    conn.execute("ALTER TABLE renamed SET SCHEMA other_schema")

    # handle_grant — GRANT then REVOKE (PUBLIC needs no role setup)
    conn.execute("GRANT SELECT ON other_schema.renamed TO PUBLIC")
    conn.execute("REVOKE SELECT ON other_schema.renamed FROM PUBLIC")
    conn.commit()

    # The renamed/moved table is still intact and usable.
    cols = conn.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = 'other_schema' AND table_name = 'renamed' "
        "ORDER BY ordinal_position"
    ).fetchall()
    assert [c[0] for c in cols] == ["id", "label"]


def test_partitioned_table_dml_and_queries_without_extension(db_without_extension):
    """Defense in depth for the planner / executor hooks. A user's *own*
    partitioned table (never deltax-managed) in a no-extension database must
    work end-to-end. This drives:
      - the ProcessUtility hook's partitioned-parent ALTER path,
      - the ExecutorStart hook (INSERT / UPDATE / DELETE),
      - the set_rel_pathlist / create_upper_paths / get_relation_info planner
        hooks (scan, aggregate, GROUP BY).
    All of these gate on the `_deltax_compressed` schema, which is absent here,
    so they must no-op — but we assert real results to be sure nothing is
    silently dropped.
    """
    conn = db_without_extension
    conn.execute(
        "CREATE TABLE events (ts timestamptz NOT NULL, val int) "
        "PARTITION BY RANGE (ts)"
    )
    conn.execute(
        "CREATE TABLE events_2025 PARTITION OF events "
        "FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')"
    )

    # ALTER on a partitioned parent — ProcessUtility classifier walks the parent.
    conn.execute("ALTER TABLE events ADD COLUMN device text")

    # DML — ExecutorStart hook.
    conn.execute(
        "INSERT INTO events (ts, val, device) VALUES "
        "('2025-06-01', 1, 'a'), ('2025-06-02', 2, 'b'), ('2025-06-03', 4, 'a')"
    )
    conn.execute("UPDATE events SET val = val + 10 WHERE device = 'b'")
    conn.execute("DELETE FROM events WHERE val = 1")
    conn.commit()

    # Scan + aggregate + GROUP BY — the planner hooks.
    total = conn.execute("SELECT count(*), sum(val) FROM events").fetchone()
    assert total == (2, 16)  # (4, val=4) and (b, val=12)
    grouped = conn.execute(
        "SELECT device, sum(val) FROM events GROUP BY device ORDER BY device"
    ).fetchall()
    assert grouped == [("a", 4), ("b", 12)]


def test_vacuum_analyze_without_extension(db_without_extension):
    """Defense in depth for the VacuumStmt branch of the ProcessUtility hook,
    which (when the extension is present) filters compressed partitions and
    restores their stats. In a no-extension database it must pass straight
    through. Both whole-database and single-table forms are exercised; VACUUM
    cannot run inside a transaction block, so use autocommit.
    """
    conn = db_without_extension
    conn.execute("CREATE TABLE t (id int, payload text)")
    conn.execute("INSERT INTO t SELECT g, 'x' FROM generate_series(1, 100) g")
    conn.commit()

    conn.autocommit = True
    conn.execute("VACUUM ANALYZE t")  # explicit-rels branch
    conn.execute("ANALYZE")  # whole-DB (rels = NIL) branch
    conn.execute("VACUUM")  # plain VACUUM, no ANALYZE
