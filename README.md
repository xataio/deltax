<p align="center">
  <a href="https://github.com/xataio/xata/blob/main/LICENSE"><img src="https://img.shields.io/badge/License-Apache_2.0-green" alt="License - Apache 2.0"></a>&nbsp;
  <a href="https://twitter.com/xata"><img src="https://img.shields.io/badge/@xata-6c47ff?label=Follow&logo=x" alt="X (formerly Twitter) Follow" /></a>&nbsp;
  <a href="https://bsky.app/profile/xata.io"><img src="https://img.shields.io/badge/@xata-6c47ff?label=Follow&logo=bluesky" alt="Bluesky Follow" /></a>&nbsp;
  <a href="https://www.youtube.com/@xataio"><img src="https://img.shields.io/badge/@xataio-6c47ff?label=Youtube&logo=youtube" alt="Youtube Subscribe" /></a>&nbsp;
</p>

# DeltaX (δx) - Fast time-series extension for PostgreSQL

DeltaX (δx) is a PostgreSQL extension offering compression and columnar storage for time-series 
data. It can be used as a pure open-source (Apache 2.0) alternative to TimescaleDB or
as a PostgreSQL-native alternative to dedicated analytics stores like ClickHouse, when
you'd like your data to stay in Postgres.

δx stores the compressed columnar data in regular Postgres tables. It does _not_ use 
its own storage format on disk. The advantage of this approach is that features like 
physical/logical replication, crash recovery, backups, and pg_dump work as for any other 
Postgres table.

## Contents

- [Benchmarks](#benchmarks)
- [How it works](#how-it-works)
- [Features](#features)
- [Limitations](#limitations)
- [Installation and quick start](#installation-and-quick-start)
- [Correctness testing](#correctness-testing)
- [Reference](#reference)
- [How can I help](#how-can-i-help)
- [License](#license)

## Benchmarks

These results are as of May 19th, 2026.

### ClickBench

On the [ClickBench](https://benchmark.clickhouse.com/) benchmark, which runs 43 analytical
queries against a web analytics dataset of 100M rows × 105 columns, δx currently ranks lower than 
specialized analytical stores like ClickHouse and DuckDB, but it is the highest-ranking of 
all the systems that are storing the data in PostgreSQL.

The following screenshot contains a selection of Postgres extensions/projects + ClickHouse for reference.
It displays the "combined" metric, which is a weighted average combining hot times, cold time, load time,
and storage size.

<img src="images/clickbench-combined.png" width="800" alt="ClickBench combined: pg_deltax ranks in-between ClickHouse and TimescaleDB">

#### Compression / storage size

Looking at the **compression ratio / storage size**, δx offers a compression ratio of about 7× on
this particular dataset. Compression ratios vary considerably by data characteristics.

<img src="images/clickbench-storage-size.png" width="800" alt="ClickBench storage size: pg_deltax compression ratio is ~7x">

#### Load time

<img src="images/clickbench-load-time.png" width="800" alt="ClickBench load times result">

Note: The reason δx can load the data faster than Postgres is that it has support for backfilling data directly
from Parquet files. On a more standard setup where the data is loaded into normal Postgres tables and
then compressed, the load time would be similar to the PostgreSQL result plus the compression time.


### JSONBench

[JSONBench](https://jsonbench.com/) is a benchmark similar to ClickBench but for measuring performance
on semi-structured data. The dataset contains Bluesky firehose data exported as ndjson.

δx has support for extracting particular fields from JSONB columns and compressing them with the same
columnar algorithms as the native columns. This enables the following results on JSONBench.

<img src="images/jsonbench-hot-run.png" width="800" alt="JSONBench hot run results">


## How it works

Let's start with an example time-series table partitioned by a timestamp column. The data itself can be metrics, 
logs, events, etc. Anything that contains a timestamp. PostgreSQL has built-in partitioning, so it's very common
to partition time-series data in fixed-interval partitions (e.g. daily, weekly, or monthly). In our example, let's
assume monthly. The partitioned table might look something like this:

<img src="images/deltax-partitioned-table.png" width="800" alt="PostgreSQL partitioned table">

Under typical time-series workloads, only the last partition (the current month) receives writes. The rest typically
only receive reads. Based on this observation, the idea is that we can compress older partitions so that they take 
less space.

<img src="images/deltax-compressed-partitions.png" width="800" alt="Compressed partitions">

A naive way to do this is to compress all the data in a given partition with a single algorithm (say, LZ4). However,
it turns out that compressing column by column has two important advantages:
- we can use type-specific compression algorithms which can be a lot more efficient in compression.
- if all the values of a given column are stored together one by one, filtering by that column becomes very efficient.

<img src="images/deltax-columnar-compress.png" width="400" alt="Switching to columnar-oriented storage during compression">

In other words, during the compression process, we also switch from row-oriented to column-oriented storage. This is 
done on a per-segment basis, meaning that each partition is split into segments of roughly equal size (by default, 30K rows)
and compressed segment by segment.

δx is currently using the following algorithms to compress the data of columns of given types:

- **Integers** (`int2`, `int4`, `int8`): tries three encodings, Constant (single repeated value), Frame-of-Reference + bit-packing (small range around a base), and Delta-Varint (variable-length encoded deltas between consecutive values), and picks whichever produces the smallest blob per segment.
- **Floats** (`float4`, `float8`): Gorilla XOR encoding (the scheme from Facebook's Gorilla paper), which exploits the fact that consecutive floats in time-series data tend to share most of their binary representation.
- **Timestamps and dates** (`timestamp`, `timestamptz`, `date`): Gorilla delta-of-delta encoding, very compact when timestamps are evenly or near-evenly spaced.
- **Booleans** (`bool`): bitmap encoding, 1 bit per value.
- **Text with low cardinality** (`text`, `varchar`, `bpchar`): dictionary encoding when cardinality is &lt; 50% of rows and &lt; 65,536 distinct values, with the dictionary indices optionally further LZ4-compressed.
- **Text with high cardinality** (`text`, `varchar`, `bpchar`): block-LZ4 over the raw strings.
- **JSONB** (`jsonb`): the raw JSONB bytes go through the same pipeline as text (dictionary or block-LZ4). In addition, when compression is enabled you can pass a `json_extract` spec to pull selected fields out of a JSONB column into synthetic columns of a chosen type (`text`, `bigint`, `timestamptz`, etc.) at compression time. These synthetic columns are then compressed with the matching type-specific codec above, just like native columns, and can be filtered, ordered, and aggregated on directly.

Across all of these, NULLs are extracted into a separate null bitmap before compression, so the codec only sees non-null values.

During compression, δx also collects metadata about the values in each segment: 

- Time bounds and row count per segment.
- Per-column min, max, sum, non-null count, and non-zero count.
- Per-column distinct-value count.
- Bloom filters for numeric, date, and timestamp columns.
- Value-presence bitmaps for low-cardinality (≤32 distinct values per partition) text columns.
- Per-row text-length sidecars: an LZ4-compressed array of character counts for every text column.

This metadata can be used during planning and execution to speed up queries, either by skipping segments that can't contribute to the result, or by answering queries directly from the metadata without touching the compressed blobs at all.

The compressed data and the metadata are stored in companion tables for each partition, with a layout carefully chosen to minimize IO for the usual access patterns. The companion tables are normal Postgres tables, meaning that they benefit from the Postgres infrastructure for replication and crash recovery. They are used transparently by the Postgres planner and executor hooks to speed up queries.

<img src="images/deltax-compressed-columnar-partitions.png" width="800" alt="DeltaX compressed columnar partitions">

An important design trade-off of δx is that compressed partitions become read-only. Writes to them are rejected and the only way to update individual rows is to decompress and re-compress the whole partition.


## Features

Current features include:

**Storage & compression**

- Auto-partitioning: turn any table with a timestamp column into a time-range partitioned table; out-of-range inserts land in a default partition.
- Per-column codecs: type-specific compression (Gorilla XOR for floats, Gorilla delta-of-delta for timestamps, Constant / FOR + bit-packing / Delta-Varint for integers, dictionary + LZ4 for text, bitmap for booleans), best codec picked per segment.
- Rich segment metadata: per-column min / max / sum / non-null / non-zero / distinct counts, bloom filters for numeric / date / timestamp columns, value-presence bitmaps for low-cardinality text, and per-row text-length sidecars.

**Query path**

- Transparent decompression: queries against compressed partitions work unchanged; the planner injects custom scan nodes that decompress on the fly.
- Segment pruning: skip whole segments using time bounds, segment-by equality, min/max, bloom filters, value-presence bitmaps, or dictionary entries — before reading the compressed blob.
- Vectorized batch filters: `=`, `<>`, `<`, `<=`, `>`, `>=`, `LIKE`, `IN` evaluated in tight Rust loops over decoded batches, bypassing PostgreSQL's per-row `ExecQual`.
- Aggregate pushdown: `COUNT(*)`, `MIN` / `MAX`, `SUM`, `AVG`, `COUNT(col)`, and `GROUP BY` answered either from segment metadata or by a vectorized aggregator inside the scan node.
- Top-N fast path: `ORDER BY ts LIMIT N` uses a two-pass scan that decodes only the sort column for most segments, then the remaining columns for the ~N winning rows.
- Parallel aggregation: parallel-aware `Partial → Gather → FinalAgg` for `SUM` / `AVG` / `COUNT` with numeric `WHERE`.
- Shared-memory blob cache: cross-backend DSA-backed cache of detoasted compressed blobs, so hot-cache scans don't pay TOAST cost.
- Text-length sidecar fast path: `length(col)` / `col = ''` / `col <> ''` queries read a few-KB sidecar instead of detoasting the multi-MB text blob.

**JSON field extraction**

- Selective JSONB field extraction: pull selected JSON paths out of a JSONB column into synthetic typed columns at compression time and compress them with the matching native codec.
- Automatic query rewrite: queries written against the original JSONB column (`data->>'field'`-style chains) are transparently rewritten to read from the synthetic columns.

**Ingest & operations**

- Direct backfill: `COPY ... WITH (FORMAT deltax_compress)` writes straight to compressed companion tables from TSV / CSV / Parquet, bypassing the heap and its WAL / index / MVCC overhead.
- Background worker: drains the default partition into proper ones, pre-creates future partitions, compresses partitions past `compress_after`, drops partitions past `drop_after`.
- PostgreSQL 17 and 18 supported.

## Limitations

- Compressed partitions are read-only. Writes are rejected; whole-partition operations (`DROP`, `TRUNCATE`) still work. If you need to update individual rows in an old partition, you must decompress, modify, and re-compress.
- No schema changes affecting column layout (`ADD` / `DROP` / `ALTER COLUMN`) on a deltatable while it has compressed partitions — you need to decompress them first, alter, and re-compress.
- No continuous (auto-refreshed) materialized aggregates yet. It is on our roadmap.
- No offloading of old partitions to S3. Data tiering is on our roadmap.
- Postgres 17 and 18 only.

## Installation and quick start

### Installation from deb file

Download the `.deb` matching your PG major version and architecture from the [latest release](https://github.com/xataio/pg_deltax/releases/latest), then:

```sh
apt-get install -y ./pg-deltax-pg17_<version>_amd64.deb
```

δx registers a background worker from `_PG_init`, so it must be in `shared_preload_libraries`:

```sh
echo "shared_preload_libraries = 'pg_deltax'" >> $PGDATA/postgresql.conf
# restart PostgreSQL, then:
psql -c "CREATE EXTENSION pg_deltax;"
```

### Installation from source

Requires a Rust toolchain, the PostgreSQL server dev headers (`postgresql-server-dev-17` or `-18` on Debian / Ubuntu), and `cargo-pgrx` matching the `pgrx` version in `Cargo.toml`:

```sh
cargo install cargo-pgrx --version 0.17.0 --locked
cargo pgrx init --pg17=$(which pg_config)
```

Then build and install the extension into the PostgreSQL instance pointed at by `pg_config`:

```sh
cargo pgrx install --release --pg-config $(which pg_config) \
    --features pg17 --no-default-features
```

Replace `pg17` with `pg18` to target PostgreSQL 18. Then add `pg_deltax` to `shared_preload_libraries`, restart PostgreSQL, and `CREATE EXTENSION pg_deltax;` as above.

### Quickstart

Either install pg_deltax as per above or run in this repo:

```sh
make run     # starts Postgres in docker with the extension loaded
make psql    # connects to it via psql
```

Then: 

pg_deltax installs all its functions and internal catalog tables into a dedicated `deltax` schema (so its `time_bucket`, `first`, `last`, etc. don't collide with TimescaleDB or pg_duckdb). Call them schema-qualified as `deltax.<fn>(...)` — or `SET search_path TO public, deltax;` once if you'd rather call them bare.

```sql
CREATE EXTENSION IF NOT EXISTS pg_deltax;
CREATE TABLE metrics (ts TIMESTAMPTZ NOT NULL, device TEXT, value FLOAT8);
SELECT deltax.deltax_create_table('metrics', 'ts', '1 day');

-- Insert ~100,000 rows spanning the last ~2.3 days across 5 devices. The past
-- (sealed) partition needs enough data that the per-partition companion-table
-- overhead is dwarfed by the compression savings — at very small scales the
-- fixed overhead can actually grow the database after compression.
INSERT INTO metrics (ts, device, value)
SELECT
    now() - (i * interval '2 seconds'),
    'sensor-' || (i % 5),
    20.0 + sin(i::float / 100) * 5
FROM generate_series(0, 100000) AS i;

-- simulate a bg worker run which would drain the defualt partition every 60s
SELECT deltax.deltax_drain_default_partition('metrics');

-- Check size and info before compression
SELECT pg_size_pretty(pg_database_size(current_database())) AS size;
SELECT * FROM deltax.deltax_partition_info('metrics');

-- Compression — compresses every partition whose window is fully in the past
-- (today's still-open partition is skipped). Normally done by the bg thread automatically.
SELECT deltax.deltax_enable_compression('metrics', order_by => ARRAY['device', 'ts']);
SELECT * FROM deltax.deltax_compress_all_partitions('metrics');
SELECT * FROM deltax.deltax_compression_stats('metrics');

-- Demo queries
SELECT deltax.time_bucket('1 day', ts) AS day, avg(value) FROM metrics GROUP BY 1 ORDER BY 1;
SELECT deltax.first(value, ts), deltax.last(value, ts) FROM metrics;

-- Size reporting after compression — both the deltatable's catalog-truthful
-- size and the whole database for comparison with the value above.
SELECT pg_size_pretty(deltax.deltax_table_size('metrics'));
SELECT pg_size_pretty(pg_database_size(current_database())) AS size;
```

## Correctness testing

The main correctness invariant in the test suite is: δx must always respond with the same results as plain Postgres returns from the uncompressed version of the table. Whenever the response is different, it is a bug. There are cases where this condition is relaxed: for example, on a `LIMIT 10` query, if the 10th row has ties, any of them is accepted. We have the following comparison policies:

- `ordered_exact` — rows and row order must match exactly.
- `unordered_exact` — row multiset must match, order is ignored.
- `limit_ties` — relaxed policy for non-unique `ORDER BY ... LIMIT` cases; boundary rows can differ as long as they're tied with rows the other side returned.
- `float_tolerant` — ordered comparison with a small numeric tolerance.

We have four layers of automated tests:

- Rust unit tests (`make test`)
- Integration tests (`make integration-test`): end-to-end tests against a running PostgreSQL with the extension loaded, run against both PG 17 and 18. They cover partitioning, compression / decompression round-trips, the background worker, parallel scans, parquet loading, JSONB field extraction, the blob cache, value bitmaps, meta-only aggregation, and more.
- Plain-PG-vs-δx correctness harness (`make correctness`): the implementation of the invariant above. Loads identical logical data into a regular PostgreSQL table and a δx table, runs the same query against both, and compares the results. The suite covers aggregates, ordering, predicates, codec round-trips via direct backfill, planner-mode coverage, partition / segment edges, joins with uncompressed tables.
- Benchmark correctness (e.g. `make -C clickbench verify`). The benchmark harnesses also act as cross-implementation parity checks, so a query that benchmarks fast but returns wrong results fails the run.

## Reference

- [Function reference](docs/FUNCTIONS.md) — partitioning, retention, compression, analytics, and blob-cache observability functions.
- [Configuration reference](docs/CONFIGURATION.md) — all `pg_deltax.*` GUCs.
- [Logical replication](docs/LOGICAL_REPLICATION.md) — setting up native PostgreSQL logical replication with pg_deltax-managed tables.

## How can I help

At the moment, the best way to contribute to this project is to:

- Spread the word: star the repo, post about it on social media, tell your friends.
- If you have a use-case in your company where δx would be beneficial, please [get in touch](mailto:info@xata.io) and we'll evaluate if δx is ready for it, or what it would take to make it ready.
- Ask your Postgres cloud provider to add support for δx. We'd like to explicitly encourage other Postgres cloud providers to adopt it.

See [CONTRIBUTING.md](CONTRIBUTING.md) for the developer guide. We recommend getting in touch before contributing new features.

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for the full text.

<br>
<p align="right">Made with 💜 by <a href="https://xata.io">Xata 🦋</a></p>
