"""Integration test for `pg_deltax.target_database` with multiple databases.

The shared session container (conftest) runs with the default config (no
`target_database`), and the GUC is Postmaster-context — so changing it needs a
full server restart, not the ALTER SYSTEM + reload trick the other worker tests
use. This test therefore spins up its *own* dedicated container configured with
a multi-entry, intentionally-duplicated list and asserts the launcher spawns
exactly one worker per *distinct* database.

`smoke_db` is created by a bind-mounted initdb script so it already exists when
the real server (and its maintenance workers) start, instead of waiting out the
60s worker restart_time for the not-yet-existing-database retry path.
"""

import os
import shutil
import subprocess
import tempfile
import time

import psycopg
import pytest

IMAGE = os.environ.get("PG_DELTAX_IMAGE")
CONTAINER = "pg_deltax_multidb_test"
PORT = int(os.environ.get("PG_DELTAX_MULTIDB_PORT", 15455))
# Duplicate `postgres` is intentional — it must be deduplicated to a single
# worker, so we expect 2 workers total, not 3.
TARGET_DATABASE = "postgres, smoke_db, postgres"


def _wait_ready(container, timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        r = subprocess.run(
            ["docker", "exec", container, "pg_isready", "-U", "postgres"],
            capture_output=True,
        )
        if r.returncode == 0:
            return
        time.sleep(1)
    raise TimeoutError(f"{container} not ready after {timeout}s")


@pytest.mark.skipif(not IMAGE, reason="PG_DELTAX_IMAGE not set")
def test_launcher_spawns_one_worker_per_distinct_database():
    # initdb scripts must live somewhere Docker Desktop shares with the VM;
    # the repo tree is already bind-mounted by the Makefile, so place it here.
    initdir = tempfile.mkdtemp(prefix=".initdb_", dir=os.path.dirname(__file__))
    try:
        with open(os.path.join(initdir, "00_create_smoke_db.sql"), "w") as f:
            f.write("CREATE DATABASE smoke_db;\n")

        subprocess.run(["docker", "rm", "-f", CONTAINER], capture_output=True)
        subprocess.check_call([
            "docker", "run", "-d",
            "--name", CONTAINER,
            "-p", f"{PORT}:5432",
            "-e", "POSTGRES_PASSWORD=postgres",
            "--shm-size=512m",
            "-v", f"{initdir}:/docker-entrypoint-initdb.d:ro",
            IMAGE,
            "-c", "shared_preload_libraries=pg_deltax",
            "-c", f"pg_deltax.target_database={TARGET_DATABASE}",
        ])
        _wait_ready(CONTAINER)

        conn = psycopg.connect(
            host="localhost", port=PORT, user="postgres",
            password="postgres", dbname="postgres", autocommit=True,
        )
        try:
            # Workers register shortly after the real server starts; poll.
            deadline = time.time() + 30
            workers = []
            while time.time() < deadline:
                workers = [
                    row[0]
                    for row in conn.execute(
                        "SELECT backend_type FROM pg_stat_activity "
                        "WHERE backend_type LIKE 'pg_deltax maintenance worker%' "
                        "ORDER BY backend_type"
                    ).fetchall()
                ]
                if len(workers) >= 2:
                    break
                time.sleep(1)
        finally:
            conn.close()

        # Exactly two: one per distinct database, in alphabetical order. The
        # duplicate `postgres` entry must NOT produce a third worker.
        assert workers == [
            "pg_deltax maintenance worker (postgres)",
            "pg_deltax maintenance worker (smoke_db)",
        ], workers
    finally:
        subprocess.run(["docker", "rm", "-f", CONTAINER], capture_output=True)
        shutil.rmtree(initdir, ignore_errors=True)
