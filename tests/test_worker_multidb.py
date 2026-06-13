"""Integration test for `pg_deltax.target_database` with multiple databases.

The shared session container (conftest) runs with the default config (no
`target_database`), and the GUC is Postmaster-context — so changing it needs a
full server restart, not the ALTER SYSTEM + reload trick the other worker tests
use. This test therefore spins up its *own* dedicated container configured with
a multi-entry, intentionally-duplicated list and asserts the launcher spawns
exactly one worker per *distinct* database.

`smoke_db` must already exist when the real server starts, otherwise its worker
crash-loops on the 60s restart_time until the database appears. We let the
official image's `POSTGRES_DB` env create it during the entrypoint's init phase
(alongside the always-present `postgres` database), so both target databases
exist by the time the pg_deltax-loaded server starts. This keeps the test to a
single container with no volume/bind-mount — earlier volume-based attempts hit
the PG18 image's data-directory layout change, and bind-mounted initdb scripts
were slow/flaky on CI.
"""

import os
import subprocess
import time

import psycopg
import pytest

IMAGE = os.environ.get("PG_DELTAX_IMAGE")
CONTAINER = "pg_deltax_multidb_test"
PORT = int(os.environ.get("PG_DELTAX_MULTIDB_PORT", 15455))
# Duplicate `postgres` is intentional — it must be deduplicated to a single
# worker, so we expect 2 workers total, not 3.
TARGET_DATABASE = "postgres, smoke_db, postgres"


def _wait_ready(container, timeout=120):
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


@pytest.mark.skipif(not IMAGE, reason="PG_DELTAX_IMAGE not set")
def test_launcher_spawns_one_worker_per_distinct_database():
    subprocess.run(["docker", "rm", "-f", CONTAINER], capture_output=True)
    try:
        # POSTGRES_DB=smoke_db creates that database during the entrypoint's
        # init phase, in addition to the built-in `postgres` — so both target
        # databases exist before the real (pg_deltax-loaded) server starts.
        subprocess.check_call([
            "docker", "run", "-d",
            "--name", CONTAINER,
            "-p", f"{PORT}:5432",
            "-e", "POSTGRES_PASSWORD=postgres",
            "-e", "POSTGRES_DB=smoke_db",
            "--shm-size=512m",
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
            # Both workers register shortly after the real server starts; poll.
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
        subprocess.run(["docker", "rm", "-f", CONTAINER], capture_output=True)
