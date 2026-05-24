# Configuration reference

All settings are PostgreSQL GUCs and follow the usual scoping rules (`SET`, `ALTER SYSTEM`, `postgresql.conf`).

## Parallelism

| GUC | Default | Context | Description |
|---|---|---|---|
| `pg_deltax.parallel_workers` | `0` | userset | Number of internal Rust worker threads used by parallel aggregation inside a custom scan. `0` = auto (CPU count, capped at 16); `1` = single-threaded; `2..=64` explicit. |
| `pg_deltax.max_parallel_workers_per_scan` | `-1` | userset | Cap on PG parallel workers for `DeltaXAppend` partial paths. `-1` follows `max_parallel_workers_per_gather`; `0` disables the partial-path variant (scans run serially); `1..=64` caps explicitly. |
| `pg_deltax.parallel_regex` | `on` | userset | When ON, compatible `REGEXP_REPLACE(...)` patterns used inside `GROUP BY` use the Rust `regex` crate so they can run thread-safe in parallel workers. |

## Blob cache (shared memory)

| GUC | Default | Context | Description |
|---|---|---|---|
| `pg_deltax.blob_cache_mb` | `-1` (auto) | postmaster | Size of the process-shared blob cache, in MiB. `-1` = auto (25% of physical RAM, clamped to `[256, 4096]`); `0` = cache disabled; `N > 0` = explicit MiB (up to 32768). Restart required — the shmem reservation is captured at postmaster start. See `dev/docs/BLOB_CACHE.md`. |
| `pg_deltax.blob_cache_shards` | `64` | postmaster | Number of shards (power of two, `1..=1024`) in the blob cache. Each shard owns an LWLock + LRU list; more shards reduce contention under high concurrency, fewer save shmem overhead. Restart required. |

## Optimization toggles

| GUC | Default | Context | Description |
|---|---|---|---|
| `pg_deltax.bloom_filters` | `on` | userset | Build per-segment bloom filters during compression for equality / `IN` predicate pushdown. Size is proportional to column cardinality (~2–5% storage overhead). Turning off applies to *new* compressions only. |
| `pg_deltax.disable_meta_agg_fastpath` | `off` | userset | When ON, `DeltaXCount` / `DeltaXMinMax` fast paths are skipped for queries with `WHERE` clauses; those queries fall through to the generic `DeltaXAgg` path instead. Used for A/B correctness comparisons. |
| `pg_deltax.disable_parallel_agg` | `off` | userset | When ON, the partial+Gather+FinalAgg path for `DeltaXAgg` is disabled and the planner only sees the complete CustomScan `DeltaXAgg`. Escape hatch for bisecting suspected regressions on the partial path; internal-Rust parallelism still runs. |
| `pg_deltax.json_extract_mode` | `none` | userset | How `COPY ... WITH (FORMAT deltax_compress)` extracts JSON paths into extra columnar columns. `none` disables extraction and the planner-side rewrite; `fields` uses the path list configured in `deltax_enable_compression(... json_extract => ...)`; `all` is reserved for auto-discovery (not yet implemented). |
| `pg_deltax.use_lz4` | `on` | userset | Declare internal columnar-blob companion columns (`_blobs._data`, `_blooms._data`, `_text_lengths._data`, `_valbitmap._bits`) with `BYTEA COMPRESSION lz4`. The columnar compression itself happens in Rust regardless; this attribute only controls the Postgres TOAST pass on those already-compressed bytes. Default ON. If the running PG was not built with `--with-lz4` the attribute is omitted automatically (and `deltax_enable_compression` emits a one-shot `WARNING` per backend) so `CREATE TABLE` doesn't fail. Set OFF explicitly to suppress the attribute on an lz4-capable build. Without lz4 the on-disk size is somewhat larger and cold-cache reads slower. |

## Testing

| GUC | Default | Context | Description |
|---|---|---|---|
| `pg_deltax.mock_now` | (empty) | suset | Override current time with a `timestamptz` literal. Empty string = use real wall-clock time. Used by the test suite to drive deterministic time-based behavior in the background worker and partition-creation paths. |
