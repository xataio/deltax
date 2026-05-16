# DeltaX (δx) - Fast time-series extension for PostgreSQL

δx is a PostgreSQL extension offering compression and columnar storage for time-series 
data. It can be used as a pure open-source (Apache 2.0) alternative to TimescaleDB or
as a PostgreSQL-native alternative to dedicated analytics stores like ClickHouse, when
you'd like your data to stay in Postgres.

δx stores the compressed data in regular Postgres tables. It does _not_ use its own storage 
format on disk. The advantage of this approach is that features like physical/logical 
replication, crash recovery, backups, and pg_dump work as for any other Postgres table.

## Benchmarks

### ClickBench

On the [ClickBench](https://benchmark.clickhouse.com/) benchmark, which runs 43 analytical
queries against a web analytics dataset of 100M rows x 105 columns, currently ranks lower than specialized analitcal stores like ClickHouse and DuckDB, but it is the highest ranking of all 
the systems that are storing the data in PostgreSQL.

![ClickBench combined: pg_deltax ranks in-between ClickHouse and TimescaleDB](images/clickbench-combined.png)

#### Compression / storage size

Looking at the **compression ratio / storage size**, δx offers compression ratio of about 7x on
this particular dataset.

![ClickBench storage size: pg_deltax compression ration is ~7x](images/clickbench-storage-size.png)

#### Cold run

![ClickBench cold run result](images/clickbench-cold-run.png)

#### Hot run

![ClickBench hot run result](images/clickbench-hot-run.png)

#### Load time

![ClickBench load times result](images/clickbench-load-time.png)

The reason δx can load the data faster than Postgres is that it has support for backfilling data directly from Parquet files. On a more standard setup where the data is loaded into normal Postgres tables and them compressed, the load time would be similar to the PostgreSQL result + the time to compress.


### JSONBench

[JSONBench](https://jsonbench.com/) is a benchmark similar to ClickBench but for measuring performance
on semi-structured data. 

δx has support for extracting particular fields from JSONB fields and compressing them with the same columnar algorithms as the native columns. This enables the following result on JSONBench.

![JSONBench hot run results](images/jsonbench-hot-run.png)

## How it works

## Correctness testing

## Features

- **Auto-partitioning**: Convert any table with a timestamp column into a partitioned deltatable
- **Background worker**: Automatically pre-creates future partitions and drains the default partition
- **`time_bucket()`**: Bucket timestamps into uniform intervals for aggregation
- **`first()` / `last()`**: Aggregates that return values associated with the earliest/latest timestamp

## Development

Requires Docker.

```sh
make test                      # run pgrx tests
make build                     # compile the extension
make clippy                    # run clippy
make cargo CMD="fmt --check"   # arbitrary cargo command
```

## Manual testing

```sh
make run    # start postgres with the extension (port 5432)
make psql   # connect to the running instance
```

## Integration tests

```sh
make integration-test                   # runs against PG 17 and 18
make integration-test PG_VERSIONS=17    # single version
```

A Python virtualenv (`.venv/`) is created automatically on first run.

## Build runtime image

```sh
make image  # builds pg_deltax:pg17
```

## Quick start

```sh
make run
# in another terminal:
psql -h localhost -U postgres -c "CREATE EXTENSION pg_deltax;"
```

```sql
CREATE TABLE metrics (ts TIMESTAMPTZ NOT NULL, device TEXT, value FLOAT8);
SELECT deltax_create_table('metrics', 'ts', '1 day');

INSERT INTO metrics VALUES (now(), 'sensor-1', 42.0);

SELECT time_bucket('1 hour', ts), avg(value) FROM metrics GROUP BY 1;
SELECT first(value, ts), last(value, ts) FROM metrics;
SELECT * FROM deltax_partition_info('metrics');

-- Compression
SELECT deltax_enable_compression('metrics', order_by => ARRAY['device', 'ts']);
SELECT deltax_compress_partition('metrics_p20250401');
SELECT * FROM deltax_compression_stats('metrics');

-- Size reporting (accounts for compressed storage)
SELECT pg_size_pretty(deltax_table_size('metrics'));
```

## Function reference

### Partitioning

| Function | Description |
|---|---|
| `deltax_create_table(relation, time_column, partition_interval DEFAULT '1 day', premake DEFAULT 3)` | Convert a table into a partitioned deltatable. Creates initial partitions around "now". |
| `deltax_partition_info(relation)` | List all partitions with their range bounds and compression status. |
| `deltax_deltatable_info(relation)` | Show metadata for a deltatable (time column, interval, partition count). |

### Retention

| Function | Description |
|---|---|
| `deltax_set_retention(relation, drop_after)` | Set a retention policy — partitions older than `drop_after` are automatically dropped by the background worker. |
| `deltax_remove_retention(relation)` | Remove the retention policy. |

### Compression

| Function | Description |
|---|---|
| `deltax_enable_compression(relation, segment_by DEFAULT '{}', order_by DEFAULT '{}', segment_size DEFAULT 30000)` | Enable compression on a deltatable. Configures how data is segmented and ordered within segments. |
| `deltax_set_compression_policy(relation, compress_after)` | Set automatic compression — partitions older than `compress_after` are compressed by the background worker. |
| `deltax_compress_partition(partition)` | Manually compress a single partition. |
| `deltax_decompress_partition(partition)` | Decompress a single partition back to heap storage. |
| `deltax_compression_stats(relation)` | Per-partition compression statistics: raw size, compressed size, ratio, row count. |
| `deltax_table_size(relation)` | Total on-disk size in bytes, accounting for compressed storage. Use with `pg_size_pretty()` for human-readable output. |

### Analytics

| Function | Description |
|---|---|
| `time_bucket(bucket_width, ts)` | Truncate a timestamp to the nearest interval boundary (like `date_trunc` but for arbitrary intervals). |
| `time_bucket(bucket_width, ts, origin)` | Same as above but with an offset (e.g., buckets starting at 06:00 instead of 00:00). |
| `first(value, ts)` | Aggregate: return the value associated with the earliest timestamp. |
| `last(value, ts)` | Aggregate: return the value associated with the latest timestamp. |

## License

Apache-2.0
