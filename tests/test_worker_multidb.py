"""Integration tests for the maintenance-worker launcher and
`pg_deltax.target_database`.

`target_database` is Postmaster-context, so it can only be set at server start
— the shared session container (conftest) runs with the default config and its
worker is continuously churned by the other worker tests, which makes asserting
"exactly these workers exist right now" racy. These tests therefore each spin
up a *fresh, dedicated* container whose worker set is stable (nothing creates or
drops deltatables in it), and assert on `pg_stat_activity.backend_type`.

`smoke_db` is created during the entrypoint's init phase via the official
image's `POSTGRES_DB` env (alongside the always-present `postgres`), so both
target databases exist before the pg_deltax server starts — no volume mount
(which broke on the PG18 image layout) and no missing-database worker retry.
"""

import os
import subprocess
import time

import psycopg
import pytest

IMAGE = os.environ.get("PG_DELTAX_IMAGE")
BASE_PORT = int(os.environ.get("PG_DELTAX_MULTIDB_PORT", 15455))

WORKER_QUERY = (
    "SELECT backend_type FROM pg_stat_activity "
    "WHERE backend_type LIKE 'pg_deltax maintenance worker%' "
    "ORDER BY backend_type"
)


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


def _worker_backend_types(container, port, env=None, server_args=None,
                          expected=1, timeout=60):
    """Boot a dedicated container, then poll until at least `expected`
    pg_deltax maintenance workers are registered (or timeout). Returns the
    sorted list of their backend_type strings. Always tears the container down.
    """
    subprocess.run(["docker", "rm", "-f", container], capture_output=True)
    cmd = [
        "docker", "run", "-d",
        "--name", container,
        "-p", f"{port}:5432",
        "-e", "POSTGRES_PASSWORD=postgres",
        "--shm-size=512m",
    ]
    for k, v in (env or {}).items():
        cmd += ["-e", f"{k}={v}"]
    cmd += [IMAGE, "-c", "shared_preload_libraries=pg_deltax"]
    cmd += server_args or []
    try:
        subprocess.check_call(cmd)
        _wait_ready(container)
        conn = psycopg.connect(
            host="localhost", port=port, user="postgres",
            password="postgres", dbname="postgres", autocommit=True,
        )
        try:
            deadline = time.time() + timeout
            workers = []
            while time.time() < deadline:
                workers = [r[0] for r in conn.execute(WORKER_QUERY).fetchall()]
                if len(workers) >= expected:
                    break
                time.sleep(1)
            return workers
        finally:
            conn.close()
    finally:
        subprocess.run(["docker", "rm", "-f", container], capture_output=True)


@pytest.mark.skipif(not IMAGE, reason="PG_DELTAX_IMAGE not set")
def test_default_config_spawns_single_postgres_worker():
    """With no `target_database` set, the launcher spawns exactly one dynamic
    maintenance worker bound to `postgres`, named per-database in
    pg_stat_activity (the pre-launcher code registered a single *static*,
    differently-named worker)."""
    workers = _worker_backend_types(
        "pg_deltax_worker_default", BASE_PORT, expected=1,
    )
    assert workers == ["pg_deltax maintenance worker (postgres)"], workers


@pytest.mark.skipif(not IMAGE, reason="PG_DELTAX_IMAGE not set")
def test_multiple_databases_spawn_one_worker_each_deduplicated():
    """A multi-entry, intentionally-duplicated `target_database` yields exactly
    one worker per *distinct* database — the duplicate `postgres` must not
    produce a third worker."""
    workers = _worker_backend_types(
        "pg_deltax_worker_multi", BASE_PORT + 1,
        env={"POSTGRES_DB": "smoke_db"},
        server_args=[
            "-c", "pg_deltax.target_database=postgres, smoke_db, postgres",
        ],
        expected=2,
    )
    assert workers == [
        "pg_deltax maintenance worker (postgres)",
        "pg_deltax maintenance worker (smoke_db)",
    ], workers
