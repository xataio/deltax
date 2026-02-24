# pg_cocoon

A PostgreSQL extension for time-series data, built on native declarative partitioning with automatic partition management.

## Features

- **Auto-partitioning**: Convert any table with a timestamp column into a partitioned hypertable
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

## Build runtime image

```sh
make image  # builds pg_cocoon:pg17
```

## Quick start

```sh
make run
# in another terminal:
psql -h localhost -U postgres -c "CREATE EXTENSION pg_cocoon;"
```

```sql
CREATE TABLE metrics (ts TIMESTAMPTZ NOT NULL, device TEXT, value FLOAT8);
SELECT cocoon_create_table('metrics', 'ts', '1 day');

INSERT INTO metrics VALUES (now(), 'sensor-1', 42.0);

SELECT time_bucket('1 hour', ts), avg(value) FROM metrics GROUP BY 1;
SELECT first(value, ts), last(value, ts) FROM metrics;
SELECT * FROM cocoon_partition_info('metrics');
```

## License

Apache-2.0
