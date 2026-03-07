# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

pg_seaturtle is a PostgreSQL extension written in Rust (using pgrx 0.17) that provides time-series data management on top of native PostgreSQL declarative partitioning. It supports PostgreSQL 14–18 (default: 17).

## Build & Test Commands

All builds run inside Docker containers via the Makefile.

```bash
make dev-image                        # Build development Docker image (required first)
make build                            # Compile the extension
make test                             # Run pgrx unit tests (PG 17)
make test PG_MAJOR=18                 # Run pgrx unit tests against specific PG version
make clippy                           # Run Rust linter
make integration-test                 # Run Python integration tests (PG 17 & 18)
make integration-test PG_VERSIONS=17  # Integration tests for specific PG version(s)
make run                              # Start PostgreSQL with extension on port 5432
make psql                             # Connect to running instance
make image                            # Build production runtime Docker image
make image-fresh                      # Rebuild runtime image with --no-cache (use after source changes)
make run-sql SQL="SELECT 1"           # Build, run SQL, show output + server logs, teardown
make run-sql-file FILE="test.sql"     # Same as run-sql but reads SQL from a file
make logs                             # Show pg_seaturtle log lines from running container
make logs-all                         # Show all logs from running container
make logs-follow                      # Follow logs in real-time
make cargo CMD="<cmd>"                # Run arbitrary cargo command in dev container
make clean                            # Clean Docker volumes
make bench-clickbench                 # Run Clickbench benchmark
make bench-clickbench-keep            # Run Clickbench benchmark, and keep the image for troubleshooting
make bench-all                        # Compare benchmarks with timescale
```

If you need to reference pgrx source code, it is in ~/src/pgrx.
If you need to reference the postgres source code, is in ~/src/postgres. 
Use the source there, it's much faster than looking into the docker images.

## Architecture

Overall design and plan with several phases is in pg_seaturtle_design_v03.md

### Rust Source (`src/`)

- **`lib.rs`** — Extension entry point. Defines `_PG_init()`, catalog table schemas (`seaturtle_hypertable`, `seaturtle_partition`) via `extension_sql!`, and the `pg_seaturtle.mock_now` GUC for testing.
- **`partition.rs`** — Core partitioning logic. Contains `seaturtle_create_table()` (converts an empty table to a partitioned hypertable), `seaturtle_partition_info()`, `seaturtle_hypertable_info()`, and helpers for interval math, partition naming, and initial partition creation.
- **`catalog.rs`** — CRUD operations against the two catalog tables using SPI. `HypertableInfo` and `PartitionInfo` structs.
- **`worker.rs`** — Background worker running every 60 seconds. Drains the default partition (moves rows to proper partitions) and pre-creates future partitions.
- **`functions/time_bucket.rs`** — `time_bucket(interval, timestamp[, origin])` for bucketing timestamps.
- **`functions/first_last.rs`** — `first(value, ts)` / `last(value, ts)` aggregates with serializable state.

### Data Flow
1. User calls `seaturtle_create_table('my_table', 'ts_column')` → table is converted to PARTITION BY RANGE, initial partitions created, metadata registered in catalog.
2. Inserts go to the parent table; PostgreSQL routes to the correct partition. Out-of-range data lands in the default partition.
3. Background worker (every 60s) drains the default partition into proper partitions and pre-creates future partitions.

### Integration Tests (`tests/`)

Python-based (pytest + psycopg). Fixtures in `conftest.py` manage Docker container lifecycle and per-test database creation. The `pg_seaturtle.mock_now` GUC allows deterministic time-based testing. Test files: `test_partitioning.py`, `test_functions.py`, `test_worker.py`.

### Unit Tests

Inline in Rust source files using `#[pg_test]` macros, run via pgrx test harness.

### Docker (`docker/`)

- `Dockerfile.dev` — Development image with Rust toolchain and pgrx CLI.
- `Dockerfile` — Multi-stage production image (compile → runtime with just PostgreSQL + extension).

## Key Patterns

- All database operations use pgrx's `Spi` abstraction (not raw SQL strings via Bash).
- Timestamps are internally represented as epoch microseconds (`i64`).
- Interval-to-microseconds conversion explicitly rejects month-based intervals.
- The background worker requires `shared_preload_libraries=pg_seaturtle` and skips execution on replicas.
