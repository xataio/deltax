"""Integration test for `pg_deltax.target_database` with multiple databases.

The shared session container (conftest) runs with the default config (no
`target_database`), and the GUC is Postmaster-context — so changing it needs a
full server restart, not the ALTER SYSTEM + reload trick the other worker tests
use. This test therefore spins up its *own* dedicated container configured with
a multi-entry, intentionally-duplicated list and asserts the launcher spawns
exactly one worker per *distinct* database.

`smoke_db` must already exist when the real server starts, otherwise its worker
crash-loops on the 60s restart_time until the database appears. We create it in
a *first* phase, on a throwaway container that does NOT load pg_deltax, sharing
a named volume with the real server. This keeps the pg_deltax-loaded server's
startup clean and fast (no missing-database worker churn during initdb) — the
earlier bind-mounted initdb-script approach loaded the extension during the
entrypoint's init phase and timed out on slower CI runners.
"""

import os
import subprocess
import time

import psycopg
import pytest

IMAGE = os.environ.get("PG_DELTAX_IMAGE")
CONTAINER = "pg_deltax_multidb_test"
INIT_CONTAINER = "pg_deltax_multidb_init"
VOLUME = "pg_deltax_multidb_vol"
PORT = int(os.environ.get("PG_DELTAX_MULTIDB_PORT", 15455))
# Duplicate `postgres` is intentional — it must be deduplicated to a single
# worker, so we expect 2 workers total, not 3.
TARGET_DATABASE = "postgres, smoke_db, postgres"


def _wait_ready(container, timeout=90):
    deadline = time.time() + timeout
    while time.time() < deadline:
        r = subprocess.run(
            ["docker", "exec", container, "pg_isready", "-U", "postgres"],
            capture_output=True,
        )
        if r.returncode == 0:
            return
        time.sleep(1)
    logs = subprocess.run(
        ["docker", "logs", "--tail", "50", container],
        capture_output=True, text=True,
    )
    raise TimeoutError(
        f"{container} not ready after {timeout}s\n--- logs ---\n"
        f"{logs.stdout}\n{logs.stderr}"
    )


def _cleanup():
    subprocess.run(["docker", "rm", "-f", CONTAINER, INIT_CONTAINER], capture_output=True)
    subprocess.run(["docker", "volume", "rm", VOLUME], capture_output=True)


@pytest.mark.skipif(not IMAGE, reason="PG_DELTAX_IMAGE not set")
def test_launcher_spawns_one_worker_per_distinct_database():
    _cleanup()  # in case a previous run left things behind
    try:
        # Phase 1: initialise the data dir WITHOUT pg_deltax loaded and create
        # smoke_db, so the real server below boots with both target databases
        # already present.
        subprocess.check_call([
            "docker", "run", "-d",
            "--name", INIT_CONTAINER,
            "-e", "POSTGRES_PASSWORD=postgres",
            "-v", f"{VOLUME}:/var/lib/postgresql/data",
            IMAGE,
        ])
        _wait_ready(INIT_CONTAINER)
        subprocess.check_call([
            "docker", "exec", INIT_CONTAINER,
            "psql", "-U", "postgres", "-c", "CREATE DATABASE smoke_db",
        ])
        subprocess.check_call(["docker", "rm", "-f", INIT_CONTAINER])

        # Phase 2: real server on the same volume, now with pg_deltax preloaded
        # and a multi-entry (duplicated) target_database list.
        subprocess.check_call([
            "docker", "run", "-d",
            "--name", CONTAINER,
            "-p", f"{PORT}:5432",
            "-e", "POSTGRES_PASSWORD=postgres",
            "--shm-size=512m",
            "-v", f"{VOLUME}:/var/lib/postgresql/data",
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
            deadline = time.time() + 60
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
        _cleanup()
