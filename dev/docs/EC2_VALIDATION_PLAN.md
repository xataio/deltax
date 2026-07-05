# EC2 validation plan — perf/clickhouse-gap-session

Validates the session's improvements on the full 100M-row ClickBench
(c6a.4xlarge, the ClickHouse reference machine). Reference numbers to
beat: ClickHouse ~33.5 s hot total; pg_deltax main ~53 s (post-#26).

> **Note:** some features referenced below — partition blooms, the
> `pg_deltax.partition_bloom_filters` and `pg_deltax.blob_storage` GUCs,
> `.dxs` files (storage-v2) — do not exist on `main` yet; they land in
> sibling PRs split from the same session. `#NN` numbers refer to the
> improvement numbering in `PERF_IMPROVEMENTS.md`, not GitHub PRs.

## 0. Setup (once)

    make -C clickbench setup EC2=<ip>           # installs PG18 + builds main, loads 100M, compresses

Run the full bench on **main** first for a same-machine baseline:

    make -C clickbench bench EC2=<ip>           # archive as baseline

## 1. Deploy the branch

    git checkout <branch-under-validation>      # a sibling PR branch, or the combined session branch
    make -C clickbench deploy EC2=<ip>          # rsync + recompile + restart

**Reload + recompress is required** (partition blooms and `.dxs` files are
built at compress time): re-run the load/compress step from setup, with
`SET pg_deltax.blob_storage = 'dual'` active for the storage-v2 numbers.

## 2. Per-improvement measurements

| Improvement | Command | Expectation vs baseline |
|---|---|---|
| Partition blooms (#47) | `make -C clickbench query EC2=<ip> Q=19` (also `query-cold`) | 43 ms → ≤15 ms; `segments_bloom_skipped` ≈ all segments of ~15/18 partitions; `meta hit` collapses |
| Merge rework (#36) | `query Q=32`, `Q=15`, `Q=35` | Q32 9.4 → ~7–8 s (merge 5.8 → ~3–4 s in DeltaX Timing); Q15/Q35 −15–30 % |
| Storage-v2 P1 | full bench with `pg_deltax.blob_storage=dual` data vs toast data | detoast term (EXPLAIN `DeltaX Timing`) ~0 on file-backed partitions; biggest on Q20–22/Q32/Q33/Q34 cold |
| Whole session | `make -C clickbench bench EC2=<ip>` (3 runs) | total ≤ ~48 s with P1 wins; the storage layer stacks toward ~35–40 s |

## 3. A/B discipline

Every new feature has a GUC kill-switch for same-data A/B:
`pg_deltax.partition_bloom_filters`, `pg_deltax.blob_storage`.
The merge rework has no GUC (pure
rewrite) — A/B it by deploying main vs branch on the same loaded data
(query-side only; no reload needed for that one).

## 4. Leaderboard page (official ClickBench format)

`make -C clickbench bench` already does this: it builds the result JSON
via `clickbench/build-result.py --template`, copies it into the upstream
checkout, runs `generate-results.sh`, and opens `index.html`. It needs a
local ClickBench clone with a `pg_deltax/` dir containing `template.json`
(model it on `timescaledb/`); the default location is `../../../ClickBench`
relative to `clickbench/` — override with `CLICKBENCH_REPO=`/`CLICKBENCH_DIR=`.

This renders pg_deltax as a column on the official leaderboard page next
to ClickHouse/Postgres/Timescale — including the geomean ranking metric
(see the caveats discussion: we can win total seconds while trailing
geomean on sub-50 ms queries).

## 5. Record

Archive `clickbench/results/history/<ts>_<commit>/` for baseline and
branch; update QUERY_ANALYSIS.md per-query table and the #36/#47/P1
sections in PERF_IMPROVEMENTS.md with measured numbers.

## Setup gap found 2026-06-12

`make -C clickbench setup` leaves PostgreSQL at stock defaults
(shared_buffers 128MB, 2-way gather) — the long-lived box had been
tuned manually. Until setup gains a tuning step, apply after install:
shared_buffers=8GB, effective_cache_size=24GB, max_worker_processes=16,
max_parallel_workers=8, max_parallel_workers_per_gather=8,
max_wal_size=8GB (ALTER SYSTEM + restart postgresql@18-main). An
untuned box reads ~2x slower on parallel-heavy queries — do not
compare untuned runs against tuned references.
