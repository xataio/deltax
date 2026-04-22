# RTABench for pg_deltax

EC2 workflow for running [RTABench](https://rtabench.com) against pg_deltax.

RTABench uses a normalized 5-table schema (customers, products, orders, order_items, order_events) modeling an online store. The big table is `order_events` (~171M rows), partitioned on `event_created`. The other four tables stay as plain Postgres tables.

We ship the 31 raw queries (0000–0030). The 1000-series (TimescaleDB continuous aggregates) is intentionally skipped — pg_deltax's value here is making the raw queries fast.

## Usage

```bash
make launch-ec2              # launch a c6a.4xlarge, prints export EC2=<ip>
make setup EC2=<ip>          # install PG18 + Rust + pgrx, build extension, load + compress data
make deploy EC2=<ip>         # iterate: rsync source, recompile, restart PG
make bench EC2=<ip>          # run all 31 queries (3x each), download results JSON
make query EC2=<ip> Q=10     # EXPLAIN ANALYZE Q10 (matches queries/0010_*.sql)
make query-cold EC2=<ip> Q=7 # same but restart PG + drop OS caches first
make sql EC2=<ip> SQL="..."
make psql EC2=<ip>
make destroy-ec2
```

Results land in `results/pg_deltax.json` and are archived to `results/history/{TIMESTAMP}_{GIT_SHA}/`.

## Configuration

`benchmark.sh` partitions `order_events` with `interval '3 days'` and 125 partitions ahead of `mock_now = 2024-01-01`, covering the full 2024 dataset. Compression uses `order_by => ['order_id', 'event_created']` and `segment_size => 30000`, mirroring the TimescaleDB baseline. Loading goes through `COPY ... FORMAT deltax_compress_csv` for direct-backfill (compress in-flight, no heap intermediate). The `_csv` variant is required because `order_events.csv` contains quoted jsonb payloads with embedded commas.

The other four tables are plain Postgres tables loaded via standard `COPY`. Only one extra index is created: `orders(customer_id)`, matching the upstream postgres baseline.
