# Function reference

## Partitioning

| Function | Description |
|---|---|
| `deltax_create_table(relation, time_column, partition_interval DEFAULT '1 day', premake DEFAULT 3)` | Convert a table into a partitioned deltatable. Creates initial partitions around "now". |
| `deltax_partition_info(relation)` | List all partitions with their range bounds and compression status. |
| `deltax_deltatable_info(relation)` | Show metadata for a deltatable (time column, interval, partition count). |
| `deltax_drain_default_partition(relation)` | Move rows that landed in `<table>_default` (because they fell outside the pre-made partition windows) into proper, time-aligned partitions. Creates the missing partitions on demand. The background worker runs the same step every 60 seconds; call this explicitly after a bulk load to skip the wait. Returns a status string with the row count moved and how many partitions were created. |

## Retention

| Function | Description |
|---|---|
| `deltax_set_retention(relation, drop_after)` | Set a retention policy — partitions older than `drop_after` are automatically dropped by the background worker. |
| `deltax_remove_retention(relation)` | Remove the retention policy. |

## Compression

| Function | Description |
|---|---|
| `deltax_enable_compression(relation, segment_by DEFAULT '{}', order_by DEFAULT '{}', segment_size DEFAULT 30000, json_extract DEFAULT NULL)` | Enable compression on a deltatable. Configures how data is segmented and ordered within segments. `json_extract` is an optional JSONB spec — `[{"src","path","name","type"}, ...]` — that pulls fields out of a JSONB column into synthetic typed columns at compression time. |
| `deltax_set_compression_policy(relation, compress_after)` | Set automatic compression — partitions older than `compress_after` are compressed by the background worker. |
| `deltax_compress_partition(partition)` | Manually compress a single partition. |
| `deltax_compress_all_partitions(relation, older_than DEFAULT NULL)` | Compress every sealed uncompressed partition of a deltatable in one call. "Sealed" means `range_end <= now()` (`pg_deltax.mock_now` is honoured when set), so the still-open current partition is never touched. With `older_than`, the threshold becomes `now() - older_than`. Returns one row `(partition_name, result)` per partition touched; empty result set means nothing was eligible. |
| `deltax_decompress_partition(partition)` | Decompress a single partition back to heap storage. |
| `deltax_analyze_partition(partition)` | Refresh `pg_class.reltuples` and `pg_statistic` for a compressed partition from the existing `_colstats` data. Useful on partitions compressed before the stats-population path shipped, or after an accidental `ANALYZE` on a compressed partition. |
| `deltax_analyze_table(relation)` | Run `deltax_analyze_partition` on every compressed partition of a deltatable. |
| `deltax_compression_stats(relation)` | Per-partition compression statistics: raw size, compressed size, ratio, row count. |
| `deltax_table_size(relation)` | Total on-disk size in bytes, accounting for compressed storage. Use with `pg_size_pretty()` for human-readable output. |

## Analytics

| Function | Description |
|---|---|
| `time_bucket(bucket_width, ts)` | Truncate a timestamp to the nearest interval boundary (like `date_trunc` but for arbitrary intervals). |
| `time_bucket(bucket_width, ts, origin)` | Same as above but with an offset (e.g., buckets starting at 06:00 instead of 00:00). |
| `first(value, ts)` | Aggregate: return the value associated with the earliest timestamp. |
| `last(value, ts)` | Aggregate: return the value associated with the latest timestamp. |

## Blob cache observability

| Function | Description |
|---|---|
| `pg_deltax_blob_cache_stats()` | Process-wide blob-cache counters: hits, misses, evictions, current bytes / entries, configured size. |
| `pg_deltax_blob_cache_shard_stats()` | Same as above but broken down per shard. Used to diagnose hot-shard contention. |
