import os
import subprocess
import time
import uuid

import psycopg
import pytest

CONTAINER_NAME = "pg_seaturtle_inttest"
HOST_PORT = 15432
PG_PASSWORD = "postgres"
PG_USER = "postgres"


@pytest.fixture(scope="session")
def pg_container():
    """Start the runtime container, wait for PG readiness, yield, then tear down."""
    image = os.environ.get("PG_SEATURTLE_IMAGE")
    if not image:
        pytest.skip("PG_SEATURTLE_IMAGE not set")

    # Clean up any leftover container from a previous run
    subprocess.run(
        ["docker", "rm", "-f", CONTAINER_NAME],
        capture_output=True,
    )

    # Start container with shared_preload_libraries so the background worker runs
    subprocess.check_call(
        [
            "docker", "run", "-d",
            "--name", CONTAINER_NAME,
            "-p", f"{HOST_PORT}:5432",
            "-e", f"POSTGRES_PASSWORD={PG_PASSWORD}",
            image,
            "-c", "shared_preload_libraries=pg_seaturtle",
        ]
    )

    # Wait for readiness
    _wait_for_pg()

    yield

    # Teardown — skip if KEEP_CONTAINER is set (for manual debugging after benchmarks)
    if os.environ.get("KEEP_CONTAINER"):
        print(f"\n  KEEP_CONTAINER set — leaving {CONTAINER_NAME} running on port {HOST_PORT}")
        print(f"  Connect with: docker exec -it {CONTAINER_NAME} psql -U {PG_USER}")
        print(f"  Or: psql -h localhost -p {HOST_PORT} -U {PG_USER}")
        print(f"  Remove with: docker rm -f {CONTAINER_NAME}")
    else:
        subprocess.run(["docker", "rm", "-f", CONTAINER_NAME], capture_output=True)


def _wait_for_pg(timeout: int = 30):
    """Poll pg_isready until the container is accepting connections."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        result = subprocess.run(
            [
                "docker", "exec", CONTAINER_NAME,
                "pg_isready", "-U", PG_USER,
            ],
            capture_output=True,
        )
        if result.returncode == 0:
            return
        time.sleep(1)
    raise TimeoutError(f"PostgreSQL not ready after {timeout}s")


def _admin_conn():
    """Return a connection to the default 'postgres' database with autocommit."""
    conn = psycopg.connect(
        host="localhost",
        port=HOST_PORT,
        user=PG_USER,
        password=PG_PASSWORD,
        dbname="postgres",
        autocommit=True,
    )
    return conn


@pytest.fixture()
def db(pg_container):
    """Create a fresh test database with the extension, yield a connection, then drop it."""
    db_name = "test_" + uuid.uuid4().hex[:12]

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
    conn.execute("CREATE EXTENSION pg_seaturtle")
    conn.commit()

    yield conn

    conn.close()

    admin = _admin_conn()
    admin.execute(f'DROP DATABASE "{db_name}"')
    admin.close()


@pytest.fixture()
def postgres_db(pg_container):
    """Connection to the postgres database where the background worker operates.

    Creates the extension if needed.  Each test gets a unique table prefix
    to avoid collisions, but cleanup is the caller's responsibility.
    """
    conn = psycopg.connect(
        host="localhost",
        port=HOST_PORT,
        user=PG_USER,
        password=PG_PASSWORD,
        dbname="postgres",
    )
    conn.execute("CREATE EXTENSION IF NOT EXISTS pg_seaturtle")
    conn.commit()

    yield conn

    # The connection may be in an error state after a failed test; roll back first
    conn.rollback()
    conn.execute("RESET pg_seaturtle.mock_now")
    conn.commit()
    conn.close()
