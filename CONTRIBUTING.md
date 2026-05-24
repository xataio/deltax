# Contributing to pg_deltax

This document covers setting up a development environment, running the test suites, and running the benchmarks.

## Prerequisites

- **Docker** — all builds run inside Docker containers via the Makefile. You don't need a local Rust toolchain or PostgreSQL install.
- A POSIX shell (macOS / Linux). Windows works via WSL2.

## Building and testing

The first time, build the dev image:

```sh
make dev-image
```

After that:

```sh
make build                            # compile the extension
make test                             # pgrx unit tests (PG 17)
make test PG_MAJOR=18                 # against a specific PG version
make clippy                           # Rust linter (includes test code)
make cargo CMD="fmt --check"          # arbitrary cargo command in the dev container
make coverage                         # unit test coverage → coverage/html/
make coverage-all                     # + integration test coverage
```

> Unit tests, integration tests, and clippy are expected to be clean on `main`. If you see a failure or warning that looks pre-existing, please fix it rather than work around it.

### Running the extension locally

```sh
make run         # start PostgreSQL with the extension on port 5432
make psql        # psql shell into the running instance
make logs        # show pg_deltax log lines
make logs-all    # full Postgres logs from the container
make logs-follow # tail logs in real time
make clean       # remove Docker volumes
```

One-shot SQL (builds, runs, prints output + server logs, tears down):

```sh
make run-sql SQL="SELECT deltax_create_table('metrics', 'ts');"
make run-sql-file FILE=path/to/file.sql
```

### Integration tests

Python-based (pytest + psycopg). Fixtures in `tests/conftest.py` manage Docker container lifecycle and create per-test databases.

```sh
make integration-test                   # runs against PG 17 and 18
make integration-test PG_VERSIONS=17    # single version
```

A Python virtualenv (`.venv/`) is created automatically on first run.

The `pg_deltax.mock_now` GUC is used throughout the worker tests to drive deterministic time-based behavior — see `tests/test_worker.py` for examples.

### Correctness harness

The plain-PG-vs-δx correctness harness loads identical logical data into both a regular Postgres table and a δx table, runs the same query against both, and compares with explicit policies (`ordered_exact`, `unordered_exact`, `limit_ties`, `float_tolerant`). See `tests/correctness/README.md` for details.

```sh
make correctness-smoke   # smoke-tagged subset
make correctness         # full curated suite
```

### Production image

```sh
make image          # build pg_deltax:pg17
make image-fresh    # same, with --no-cache (rebuild after source changes)
```

## Benchmarks

There are two benchmark environments: **local** (Docker, small data subset, faster feedback, doubles as a correctness check) and **full** (EC2, complete dataset). For most changes that might affect performance, run both.

### ClickBench

#### Local (Docker)

```sh
make image-fresh                # rebuild production image with your changes
make bench-clickbench-keep      # run benchmark (~2 min), keeps container running
```

The benchmark prints a `psql postgres://...` connection string at the end — use it for `EXPLAIN ANALYZE` and ad-hoc queries against the benchmark DB.

#### Full (EC2)

Runs from `clickbench/Makefile` against a remote EC2 instance with the complete 100M-row dataset. The EC2 IP is read from `clickbench/.env`.

```sh
# First-time setup (installs PG18, Rust, pgrx, builds + loads + compresses)
make -C clickbench setup EC2=<ip>

# Iterating
make -C clickbench deploy EC2=<ip>          # rsync source, recompile, restart PG
make -C clickbench bench EC2=<ip>           # all 43 queries × 3 runs each

# Investigating specific queries
make -C clickbench query EC2=<ip> Q=33                # EXPLAIN ANALYZE one query
make -C clickbench query-cold EC2=<ip> Q=7            # cold caches (restart PG + drop OS caches)
make -C clickbench query EC2=<ip> Q=33 SET="SET pg_deltax.parallel_workers=4"

# Ad-hoc
make -C clickbench sql EC2=<ip> SQL="SHOW work_mem"
make -C clickbench psql EC2=<ip>
make -C clickbench ssh EC2=<ip>
```

Results land in `clickbench/results/pg_deltax.json` (latest) + `clickbench/results/history/<TS>_<commit>/` (archive). **ClickBench queries are numbered starting at 0.**

### RTABench

[RTABench](https://rtabench.com) is a real-time-analytics benchmark on a normalized 5-table schema — a counterpoint to ClickBench's single wide denormalized table. 31 raw queries; the 10 pre-aggregated `1000_*` queries rely on TimescaleDB continuous aggregates and are intentionally skipped.

Query-by-query analysis lives in [`dev/docs/RTABENCH_QUERY_ANALYSIS.md`](dev/docs/RTABENCH_QUERY_ANALYSIS.md). RTABench queries are numbered starting at 0 (`00NN_<name>.sql`).

#### Local (Docker)

Side-by-side plain-PG vs δx comparison on a sub-GB slice. Doubles as a correctness test — requires byte-identical results.

```sh
make bench-rtabench-keep        # wipe PG volume, reload + recompress, keep container running
make bench-rtabench-full        # same with full 10M-order dataset (slow, for parity with EC2)
make bench-rtabench-clean       # remove container + PG volume, keep CSV cache
make bench-rtabench-distclean   # also remove the ~7 GB CSV cache (forces redownload)
```

First run downloads ~7 GB of upstream CSVs (one-time, cached). Subsequent runs take ~1–2 min.

Override `RTABENCH_ORDERS=<n>` to change the slice size (default 250,000).

Results in `tests/.bench_results/rtabench_pg_deltax.json` (latest) + `tests/.bench_results/history/`.

#### Full (EC2)

Runs from `rtabench/Makefile` on a remote EC2 with the complete ~181M-event dataset. IP from `rtabench/.env`.

```sh
make -C rtabench setup EC2=<ip>          # first-time install + load + compress
make -C rtabench deploy EC2=<ip>         # rsync + recompile + restart PG
make -C rtabench bench EC2=<ip>          # all 31 queries × 3 runs, generate comparison HTML
make -C rtabench report                  # regenerate HTML from existing results

make -C rtabench query EC2=<ip> Q=17
make -C rtabench query-cold EC2=<ip> Q=17

make -C rtabench fetch-competitors       # one-time: pull Postgres/Timescale/ClickHouse/... JSONs from rtabench.com
```

`make bench` saves to `rtabench/results/pg_deltax.json` + history, and generates a comparison HTML with every system side-by-side.

### JSONBench

[JSONBench](https://github.com/ClickHouse/JSONBench) tests JSON-heavy analytical workloads using up to 1B Bluesky firehose events. **EC2-only** (no Docker variant). Upstream supports four scales (1m, 10m, 100m, 1000m).

The pg_deltax schema extracts a top-level `ts TIMESTAMPTZ` from `data->>'time_us'` so we can partition + sort on it; the rest of the JSONB stays untouched. Queries in `jsonbench/queries.sql` (5 total, indexed from 0).

```sh
# First-time setup on m6i.8xlarge
make -C jsonbench setup EC2=<ip>            # SCALE=100 default (100m rows)
make -C jsonbench setup EC2=<ip> SCALE=1    # 1m smoke test
make -C jsonbench setup EC2=<ip> SCALE=1000 # full 1B rows (hours)

make -C jsonbench deploy EC2=<ip>
make -C jsonbench bench EC2=<ip>
make -C jsonbench query EC2=<ip> Q=2
make -C jsonbench query-cold EC2=<ip> Q=2

make -C jsonbench sql EC2=<ip> SQL="SELECT count(*) FROM bluesky"
make -C jsonbench psql EC2=<ip>
```

Results in `jsonbench/results/pg_deltax.json` + history.

> **Schema caveat**: the extracted `ts` column and the absence of upstream's functional B-tree index mean pg_deltax results aren't strictly apples-to-apples with the upstream `postgresql/` JSONBench numbers — pg_deltax relies on segment-level minmax pruning over the sort key instead. Qualify any side-by-side comparison.
