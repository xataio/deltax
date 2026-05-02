# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

pg_deltax is a PostgreSQL extension written in Rust (using pgrx 0.17) that provides time-series data management on top of native PostgreSQL declarative partitioning. It supports PostgreSQL 14–18 (default: 17).

## Build & Test Commands

All builds run inside Docker containers via the Makefile.

```bash
make dev-image                        # Build development Docker image (required first)
make build                            # Compile the extension
make test                             # Run pgrx unit tests (PG 17)
make test PG_MAJOR=18                 # Run pgrx unit tests against specific PG version
make clippy                           # Run Rust linter (includes test code)
make coverage                         # Unit test coverage report → coverage/html/
make coverage-all                     # Unit + integration test coverage → coverage/html/
make integration-test                 # Run Python integration tests (PG 17 & 18)
make integration-test PG_VERSIONS=17  # Integration tests for specific PG version(s)
make run                              # Start PostgreSQL with extension on port 5432
make psql                             # Connect to running instance
make image                            # Build production runtime Docker image
make image-fresh                      # Rebuild runtime image with --no-cache (use after source changes)
make run-sql SQL="SELECT 1"           # Build, run SQL, show output + server logs, teardown
make run-sql-file FILE="test.sql"     # Same as run-sql but reads SQL from a file
make logs                             # Show pg_deltax log lines from running container
make logs-all                         # Show all logs from running container
make logs-follow                      # Follow logs in real-time
make cargo CMD="<cmd>"                # Run arbitrary cargo command in dev container
make clean                            # Clean Docker volumes
make bench-clickbench                 # Run Clickbench benchmark
make bench-clickbench-keep            # Run Clickbench benchmark, keep container running
make bench-rtabench                   # Run RTABench (plain PG vs pg_deltax) on a 250K-order subset
make bench-rtabench-keep              # Wipe DB volume (not CSV cache), reload + recompress, keep container running
make bench-rtabench-full              # RTABench with full 10M orders locally (slow, for parity with EC2)
make bench-rtabench-clean             # Remove RTABench container + PG volume (preserves CSV cache)
make bench-rtabench-distclean         # Full wipe including the ~7 GB CSV cache (forces redownload)
make bench-clean                      # Remove benchmark data volume
make bench-all                        # Compare benchmarks with timescale
```

### Benchmark Workflow

There are two benchmark environments: **local** (Docker, small data subset, more checks) and **full** (EC2, complete ClickBench dataset). On most changes, run both.

#### Local Benchmark (Docker)

1. `make image-fresh` — rebuild the production image with your code changes
2. `make bench-clickbench-keep` — run the benchmark (~2 min), keeps container running
3. Use the connection string printed at the end to run EXPLAIN ANALYZE or ad-hoc queries against the benchmark DB

The benchmark prints a `psql postgres://...` connection string at the end. Use it to investigate specific queries with EXPLAIN ANALYZE, verify plan choices, etc.

#### Full Benchmark (EC2)

Runs from `clickbench/Makefile` against a remote EC2 instance with the complete
ClickBench dataset (~100M rows). The EC2 IP is saved in the `.env` file in the
clickbench folder.

```bash
# First-time setup (installs PG18, Rust, pgrx, builds extension, loads data, compresses)
make -C clickbench setup EC2=<ip>

# Iterating on code changes
make -C clickbench deploy EC2=<ip>          # rsync source, recompile, restart PG
make -C clickbench bench EC2=<ip>           # run all 43 queries (3 runs each), download results

# Investigating specific queries
make -C clickbench query EC2=<ip> Q=33      # EXPLAIN ANALYZE a single query
make -C clickbench query-cold EC2=<ip> Q=7  # same but with cold caches (restarts PG, drops OS caches)
make -C clickbench query EC2=<ip> Q=33 SET="SET pg_deltax.parallel_workers=4"  # with GUC overrides

# Ad-hoc
make -C clickbench sql EC2=<ip> SQL="SHOW work_mem"
make -C clickbench psql EC2=<ip>
make -C clickbench ssh EC2=<ip>
```

Results are saved to `clickbench/results/pg_deltax.json` and archived by timestamp+commit in `clickbench/results/history/`.

### RTABench Workflow

[RTABench](https://rtabench.com) is a real-time-analytics benchmark built on a normalized 5-table schema (customers, products, orders, order_items, order_events) — a counterpoint to ClickBench's single wide denormalized table. 31 raw queries; pg_deltax only targets those (the 10 pre-aggregated `1000_*` queries rely on TimescaleDB continuous aggregates and are intentionally skipped).

Full query-by-query analysis with plan breakdowns and root causes for slow queries lives in `dev/docs/RTABENCH_QUERY_ANALYSIS.md`. RTABench queries are counted starting with 00 (filenames are `00NN_<name>.sql`).

#### Local RTABench (Docker)

Side-by-side plain-PG vs pg_deltax comparison on a sub-GB slice of the real dataset. Runs every query against both `order_events_plain` (plain PostgreSQL) and `order_events` (pg_deltax-managed, compressed) and requires result-set equality — so it doubles as a correctness test.

1. `make bench-rtabench-keep` — wipes the previous PG volume, reloads data through the current extension code (so compression changes are exercised), and leaves the container up. First run downloads ~7 GB of upstream CSVs (one-time). Subsequent runs reuse the cache and take ~1–2 min.
2. `docker exec -it pg_deltax_inttest psql -U postgres -d bench_rtabench` — poke at plans directly after the run.
3. `make bench-rtabench-clean` — wipes the container + PG volume, keeps the CSV cache. `make bench-rtabench-distclean` also removes the CSV cache (forces ~7 GB redownload).

Override `RTABENCH_ORDERS=<n>` to change the slice size (default 250,000; use `bench-rtabench-full` for all 10M).

Results go to `tests/.bench_results/rtabench_pg_deltax.json` (latest) + `.bench_results/history/<TS>_<commit>/`.

#### Full RTABench (EC2)

Runs from `rtabench/Makefile` against a remote EC2 instance with the complete
dataset (~181M events). The EC2 ip address is found in the .env file in the
rtabench folder. Try that one before launching a new one.

```bash
# First-time setup (installs PG18, Rust, pgrx, builds extension, loads + compresses data)
make -C rtabench setup EC2=<ip>

# Iterating on code changes
make -C rtabench deploy EC2=<ip>          # rsync source, recompile, restart PG
make -C rtabench bench EC2=<ip>           # run all 31 queries (3x each), generate comparison HTML
make -C rtabench report                   # regenerate HTML from existing results

# Investigating specific queries (zero-padded number; Q=10 → queries/0010_*.sql)
make -C rtabench query EC2=<ip> Q=17
make -C rtabench query-cold EC2=<ip> Q=17 # restart PG + drop OS caches first

# Ad-hoc
make -C rtabench sql EC2=<ip> SQL="SHOW work_mem"
make -C rtabench psql EC2=<ip>

# Comparison HTML — pulls competitor results from rtabench.com
make -C rtabench fetch-competitors        # one-time: drops Postgres/TimescaleDB/ClickHouse/... JSONs under ~/src/rtabench/<system>/results/
```

`make bench` saves `rtabench/results/pg_deltax.json`, archives by timestamp+commit in `rtabench/results/history/`, copies the JSON to `~/src/rtabench/pg_deltax/results/c6a.4xlarge.json` (ClickBench-style mirror layout), and generates `~/src/rtabench/index.html` with every system as a comparison column.

### JSONBench Workflow

[JSONBench](https://github.com/ClickHouse/JSONBench) tests JSON-heavy analytical workloads using 1B Bluesky events (ndjson). Upstream supports four scales (1m, 10m, 100m, 1000m). The pg_deltax harness extends the upstream PostgreSQL schema with a top-level `ts TIMESTAMPTZ` column extracted from `data->>'time_us'` so pg_deltax can partition and order on it; the JSONB payload otherwise stays untouched. EC2-only (no Docker variant). Queries live in `jsonbench/queries.sql` (one per line, 5 total — index from 0).

```bash
# First-time setup on m6i.8xlarge (JSONBench reference machine)
make -C jsonbench setup EC2=<ip>                # SCALE=100 default (100m rows)
make -C jsonbench setup EC2=<ip> SCALE=1        # 1m smoke test
make -C jsonbench setup EC2=<ip> SCALE=1000     # full 1B rows (hours)

# Iterating
make -C jsonbench deploy EC2=<ip>               # rsync source, recompile, restart PG
make -C jsonbench bench EC2=<ip>                # run all 5 queries (3x each), download results

# Investigating specific queries (Q=0..4)
make -C jsonbench query EC2=<ip> Q=2
make -C jsonbench query-cold EC2=<ip> Q=2

# Ad-hoc
make -C jsonbench sql EC2=<ip> SQL="SELECT count(*) FROM bluesky"
make -C jsonbench psql EC2=<ip>
```

Results land in `jsonbench/results/pg_deltax.json` and are archived by timestamp+commit in `jsonbench/results/history/`. If `JSONBENCH_DIR` (default `/Users/tsg/src/JSONBench`) exists, the result is also copied to `<JSONBENCH_DIR>/postgresql/results/pg_deltax_m6i.8xlarge_<scale>.json` so the upstream dashboard generator picks it up.

Schema caveat: the extracted `ts` column and absence of upstream's functional B-tree index mean pg_deltax results aren't a strict apples-to-apples comparison with the upstream `postgresql/` JSONBench numbers — pg_deltax relies on segment-level minmax pruning over the sort key instead. Qualify any side-by-side comparisons with this.

If you need to reference pgrx source code, it is in ~/src/pgrx.
If you need to reference the postgres source code, is in ~/src/postgres.
Use the source there, it's much faster than looking into the docker images.
Remember that ClickBench queries are counted starting with 0.

## Architecture

You can find some design docs under ./dev/docs:
- ARCHITECTURE.md - high level architecture.
- COLUMNAR_STORAGE.md - how the data is organized in PG tables.
- PERF_IMPROVEMENTS.md - a long list of performance optimizations we applied.

### Data Flow
1. User calls `deltax_create_table('my_table', 'ts_column')` → table is converted to PARTITION BY RANGE, initial partitions created, metadata registered in catalog.
2. Inserts go to the parent table; PostgreSQL routes to the correct partition. Out-of-range data lands in the default partition.
3. Background worker (every 60s) drains the default partition into proper partitions and pre-creates future partitions.

### Integration Tests (`tests/`)

Python-based (pytest + psycopg). Fixtures in `conftest.py` manage Docker container lifecycle and per-test database creation. The `pg_deltax.mock_now` GUC allows deterministic time-based testing. Test files: `test_partitioning.py`, `test_functions.py`, `test_worker.py`.

### Unit Tests

Inline in Rust source files using `#[pg_test]` macros, run via pgrx test harness.

Unit, integration, and clippy are supposed to always be clean. Even if you
suspect the failures/warnings are pre-existing, work on fixing them, rather
than ignoring them.

### Docker (`docker/`)

- `Dockerfile.dev` — Development image with Rust toolchain and pgrx CLI.
- `Dockerfile` — Multi-stage production image (compile → runtime with just PostgreSQL + extension).

## Key Patterns

- All database operations use pgrx's `Spi` abstraction (not raw SQL strings via Bash).
- Timestamps are internally represented as epoch microseconds (`i64`).
- Interval-to-microseconds conversion explicitly rejects month-based intervals.
- The background worker requires `shared_preload_libraries=pg_deltax` and skips execution on replicas.
