"""Integration tests for session mode (`session_preload_libraries`).

The setup these tests exercise (CREATE EXTENSION → ALTER DATABASE SET
session_preload_libraries → deltax_run_maintenance()) is the same flow
documented for users in docs/PRELOAD_MODES.md; keep the two in sync.

These are the only tests that exercise the `else` branch of `_PG_init` — i.e.
pg_deltax loaded *without* `shared_preload_libraries`. They therefore run their
own dedicated container started without shared_preload, on a separate port, so
the failure modes that only exist when the library isn't postmaster-loaded
(no background worker; hook-less backends; inert PGC_POSTMASTER GUCs) are real.

Covered:
- CREATE EXTENSION works with no preload.
- The maintenance background worker is NOT registered in session mode.
- `session_preload_libraries` installs the query hooks at connect, so a plain
  SELECT from a fresh connection reconstructs compressed rows.
- The documented limitation (§3 of dev/docs/PRELOAD_MODES.md): a hook-less
  backend silently returns zero rows from compressed partitions — and LOAD
  fixes it.
- `deltax_run_maintenance()` runs the full pass in session mode (no worker).
- The PGC_POSTMASTER GUCs are inert (cannot be SET per-session).
- The custom scan stays correct under forced parallelism in session mode.
"""

import os
import subprocess
import time
import uuid

import psycopg
import pytest

IMAGE = os.environ.get("PG_DELTAX_IMAGE")
CONTAINER = "pg_deltax_session_inttest"
PORT = int(os.environ.get("PG_DELTAX_SESSION_PORT", 15433))
PG_USER = "postgres"
PG_PASSWORD = "postgres"

pytestmark = pytest.mark.skipif(IMAGE is None, reason="PG_DELTAX_IMAGE not set")


def _connect(dbname, autocommit=False):
    return psycopg.connect(
        host="localhost",
        port=PORT,
        user=PG_USER,
        password=PG_PASSWORD,
        dbname=dbname,
        autocommit=autocommit,
    )


def _wait_ready(timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        r = subprocess.run(
            ["docker", "exec", CONTAINER, "pg_isready", "-U", PG_USER],
            capture_output=True,
        )
        if r.returncode == 0:
            return
        time.sleep(1)
    raise TimeoutError("session-mode PostgreSQL not ready")


@pytest.fixture(scope="module")
def session_container():
    """A PG container started WITHOUT shared_preload_libraries.

    This is the whole point: only here is `_PG_init` reached via
    session_preload / LOAD / fmgr rather than a postmaster shared-preload load.
    """
    subprocess.run(["docker", "rm", "-f", CONTAINER], capture_output=True)
    subprocess.check_call(
        [
            "docker", "run", "-d",
            "--name", CONTAINER,
            "-p", f"{PORT}:5432",
            "-e", f"POSTGRES_PASSWORD={PG_PASSWORD}",
            "--shm-size=256m",
            IMAGE,
            # Deliberately NO `-c shared_preload_libraries=pg_deltax`.
        ]
    )
    try:
        _wait_ready()
        yield
    finally:
        if os.environ.get("KEEP_CONTAINER"):
            print(f"\n  KEEP_CONTAINER set — leaving {CONTAINER} on port {PORT}")
        else:
            subprocess.run(["docker", "rm", "-f", CONTAINER], capture_output=True)


@pytest.fixture()
def session_db(session_container):
    """A fresh database with no extension and no preload configured yet."""
    name = "sess_" + uuid.uuid4().hex[:12]
    admin = _connect("postgres", autocommit=True)
    admin.execute(f'CREATE DATABASE "{name}"')
    admin.close()

    yield name

    admin = _connect("postgres", autocommit=True)
    # ALTER DATABASE ... SET session_preload can leave backends connecting; make
    # sure none linger before DROP.
    admin.execute(
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
        "WHERE datname = %s AND pid <> pg_backend_pid()",
        (name,),
    )
    admin.execute(f'DROP DATABASE IF EXISTS "{name}"')
    admin.close()


def _set_db_session_preload(db_name):
    admin = _connect("postgres", autocommit=True)
    admin.execute(
        f'ALTER DATABASE "{db_name}" '
        "SET session_preload_libraries = 'pg_deltax'"
    )
    admin.close()


def _build_compressed_table(conn, table="sessmetrics"):
    """Create a deltatable with all rows in one partition, compress it, and
    return the row count. The connection must already have hooks (so it can
    create + read); the heap of the compressed partition ends up truncated."""
    conn.execute("SET pg_deltax.mock_now = '2025-03-10 12:00:00+00'")
    conn.execute(
        f"CREATE TABLE {table} "
        "(ts timestamptz not null, device text not null, val float8)"
    )
    conn.execute(
        f"SELECT deltax.deltax_create_table('{table}', 'ts', '1 day'::interval)"
    )
    conn.commit()

    rows = [
        f"('2025-03-10 00:00:00+00'::timestamptz + interval '{i} minutes', "
        f"'d{i % 5}', {i})"
        for i in range(200)
    ]
    conn.execute(
        f"INSERT INTO {table} (ts, device, val) VALUES " + ", ".join(rows)
    )
    conn.commit()

    conn.execute(
        f"SELECT deltax.deltax_enable_compression('{table}', "
        "segment_by => ARRAY['device'], order_by => ARRAY['ts'])"
    )
    conn.commit()

    part = conn.execute(
        f"SELECT partition_name FROM deltax.deltax_partition_info('{table}') "
        "WHERE range_start <= '2025-03-10'::timestamptz "
        "AND range_end > '2025-03-10'::timestamptz"
    ).fetchone()[0]
    conn.execute(f"SELECT deltax.deltax_compress_partition('{part}')")
    conn.commit()

    # Writer has hooks → sees every row.
    n = conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
    assert n == 200, f"writer with hooks should see all rows, got {n}"
    return 200


def test_fixture_really_has_no_shared_preload(session_db):
    """Guard rail: confirm we are actually testing session mode."""
    conn = _connect(session_db)
    val = conn.execute("SHOW shared_preload_libraries").fetchone()[0]
    assert "pg_deltax" not in (val or ""), (
        "this suite is meaningless if pg_deltax is shared-preloaded"
    )
    conn.close()


def test_create_extension_without_preload(session_db):
    """CREATE EXTENSION must work with no preload at all (catalog + functions)."""
    conn = _connect(session_db)
    conn.execute("CREATE EXTENSION pg_deltax")
    conn.commit()
    n = conn.execute(
        "SELECT count(*) FROM deltax.deltax_deltatable"
    ).fetchone()[0]
    assert n == 0
    # A pg_deltax C function is callable (lazy fmgr load runs _PG_init).
    conn.execute("CREATE TABLE t (ts timestamptz not null, v float8)")
    conn.execute("SELECT deltax.deltax_create_table('t', 'ts', '1 day'::interval)")
    conn.commit()
    assert conn.execute(
        "SELECT count(*) FROM deltax.deltax_deltatable"
    ).fetchone()[0] == 1
    conn.close()


def test_no_background_worker_in_session_mode(session_db):
    """Session mode must not register the maintenance worker — the static
    RegisterBackgroundWorker is gated behind shared-preload."""
    conn = _connect(session_db)
    conn.execute("CREATE EXTENSION pg_deltax")
    conn.commit()
    # Load the library in-session; it still must not spawn a worker.
    conn.execute("LOAD 'pg_deltax'")
    workers = conn.execute(
        "SELECT count(*) FROM pg_stat_activity "
        "WHERE backend_type ILIKE '%pg_deltax%' "
        "   OR application_name ILIKE '%pg_deltax%'"
    ).fetchone()[0]
    assert workers == 0, "session mode must not run a background worker"
    conn.close()


def test_session_preload_enables_hooks_for_plain_select(session_db):
    """With `session_preload_libraries` set on the database, a brand-new
    connection that only runs SELECT (never calls a deltax function) still has
    the custom-scan hook and reconstructs compressed rows."""
    _set_db_session_preload(session_db)

    writer = _connect(session_db)
    writer.execute("CREATE EXTENSION pg_deltax")
    writer.commit()
    total = _build_compressed_table(writer)
    writer.close()

    reader = _connect(session_db)
    cnt = reader.execute("SELECT count(*) FROM sessmetrics").fetchone()[0]
    assert cnt == total, (
        "a session_preload reader doing only SELECT must see compressed rows"
    )
    reader.close()


def test_silent_empty_without_hooks_is_known_limitation(session_db):
    """KNOWN LIMITATION (dev/docs/PRELOAD_MODES.md §3): a hook-less backend
    silently returns zero rows from compressed partitions. This pins that exact
    behavior — and that loading the library fixes it. If a future change makes
    this raise instead of returning 0, update §3 and this test together."""
    # No session_preload on the DB → fresh connections are hook-less. The writer
    # loads the lib via fmgr (calling deltax functions) which is enough to build.
    writer = _connect(session_db)
    writer.execute("CREATE EXTENSION pg_deltax")
    writer.commit()
    total = _build_compressed_table(writer)
    writer.close()
    assert total > 0

    reader = _connect(session_db)
    empty = reader.execute("SELECT count(*) FROM sessmetrics").fetchone()[0]
    assert empty == 0, (
        "expected the documented silent-empty behavior from a hook-less backend"
    )

    # Loading pg_deltax installs the hook; the same connection now reads fully.
    reader.execute("LOAD 'pg_deltax'")
    fixed = reader.execute("SELECT count(*) FROM sessmetrics").fetchone()[0]
    assert fixed == total, "after LOAD the same connection reconstructs the rows"
    reader.close()


def test_run_maintenance_in_session_mode(session_db):
    """deltax_run_maintenance() runs the full pass with no background worker.
    Deterministic: nothing else can drain the default, so a single call must."""
    _set_db_session_preload(session_db)

    conn = _connect(session_db)
    conn.execute("CREATE EXTENSION pg_deltax")
    conn.commit()
    conn.execute("SET pg_deltax.mock_now = '2025-04-01 00:00:00+00'")
    conn.execute("CREATE TABLE m (ts timestamptz not null, v float8)")
    conn.execute("SELECT deltax.deltax_create_table('m', 'ts', '1 day'::interval, 1)")
    conn.commit()

    # Future row → default partition.
    conn.execute("INSERT INTO m VALUES ('2025-05-01 12:00:00+00', 1.0)")
    conn.commit()
    assert conn.execute("SELECT count(*) FROM m_default").fetchone()[0] == 1

    # Advance the clock and drive maintenance. No worker exists in session mode,
    # so this is the only thing that can drain the default.
    conn.execute("SET pg_deltax.mock_now = '2025-05-01 00:00:00+00'")
    conn.execute("SELECT deltax.deltax_run_maintenance()")
    conn.commit()

    assert conn.execute("SELECT count(*) FROM m_default").fetchone()[0] == 0, (
        "deltax_run_maintenance() must drain the default in session mode"
    )
    assert conn.execute("SELECT count(*) FROM m").fetchone()[0] == 1
    conn.close()


def test_run_maintenance_preserves_caller_search_path(session_db):
    """deltax_run_maintenance() pins search_path with SET LOCAL while it runs
    (so unqualified names can't be shadowed), but must NOT leak that into the
    caller's session — proving SET LOCAL, not a plain SET."""
    _set_db_session_preload(session_db)
    conn = _connect(session_db)
    conn.execute("CREATE EXTENSION pg_deltax")
    conn.commit()

    conn.execute("SET search_path = public, pg_catalog")
    before = conn.execute("SHOW search_path").fetchone()[0]
    conn.execute("SELECT deltax.deltax_run_maintenance()")
    conn.commit()
    after = conn.execute("SHOW search_path").fetchone()[0]

    assert after == before, (
        "deltax_run_maintenance() must not leak its search_path pin "
        f"(was {before!r}, now {after!r})"
    )
    conn.close()


def test_postmaster_gucs_absent_in_session_mode(session_db):
    """The PGC_POSTMASTER GUCs (target_database, blob_cache_mb, blob_cache_shards)
    are NOT defined in session mode: _PG_init must skip them because PostgreSQL
    FATALs if a PGC_POSTMASTER variable is created outside postmaster startup.
    The USERSET GUCs are still defined and usable.

    That `LOAD 'pg_deltax'` below succeeds at all is the core regression check —
    before the fix it FATAL'd with 'cannot create PGC_POSTMASTER variables after
    startup'."""
    conn = _connect(session_db)
    conn.execute("CREATE EXTENSION pg_deltax")
    conn.commit()
    conn.execute("LOAD 'pg_deltax'")  # runs _PG_init in this backend — must not FATAL

    # The three full-mode-only knobs are simply absent.
    for guc in [
        "pg_deltax.blob_cache_mb",
        "pg_deltax.blob_cache_shards",
        "pg_deltax.target_database",
    ]:
        with pytest.raises(Exception, match="unrecognized configuration parameter"):
            conn.execute(f"SHOW {guc}")
        conn.rollback()

    # USERSET GUCs are present and settable — the query-path knobs still work.
    assert conn.execute("SHOW pg_deltax.parallel_workers").fetchone()[0] is not None
    conn.execute("SET pg_deltax.parallel_workers = 3")
    assert conn.execute("SHOW pg_deltax.parallel_workers").fetchone()[0] == "3"
    conn.close()


def test_custom_scan_parallel_correct_in_session_mode(session_db):
    """The custom scan must stay correct under parallelism in session mode —
    Postgres replays the leader's loaded-library set into parallel workers, so
    each worker re-runs _PG_init and installs the hook."""
    _set_db_session_preload(session_db)

    writer = _connect(session_db)
    writer.execute("CREATE EXTENSION pg_deltax")
    writer.commit()
    total = _build_compressed_table(writer)
    writer.close()

    reader = _connect(session_db)
    # Push the planner hard toward a parallel plan.
    reader.execute("SET max_parallel_workers_per_gather = 2")
    reader.execute("SET parallel_setup_cost = 0")
    reader.execute("SET parallel_tuple_cost = 0")
    reader.execute("SET min_parallel_table_scan_size = 0")

    cnt = reader.execute("SELECT count(*) FROM sessmetrics").fetchone()[0]
    assert cnt == total

    groups = reader.execute(
        "SELECT device, count(*) FROM sessmetrics GROUP BY device ORDER BY device"
    ).fetchall()
    assert sum(g[1] for g in groups) == total
    assert len(groups) == 5
    reader.close()
