# Schema changes

pg_deltax intercepts DDL on δx partitioned tables so that operations which are safe against compressed partitions pass through transparently, and operations whose semantics would silently diverge on compressed data are rejected with a clear error.

If a table has no compressed partitions, every ALTER behaves exactly like plain PostgreSQL.

## Transactional behavior

DDL on δx tables is fully transactional, the same as plain PostgreSQL DDL. The ALTER and any pg_deltax catalog bookkeeping it triggers (renames mirrored into `segment_by` / `order_by` / `time_column`, descriptor tombstones on `DROP COLUMN`, OWNER / GRANT cascades onto companion tables) all run inside the calling transaction:

```sql
BEGIN;
ALTER TABLE metrics RENAME COLUMN host TO hostname;
ROLLBACK;  -- both the rename and the catalog mirror are undone
```

Blocked operations raise `ERRCODE_FEATURE_NOT_SUPPORTED` before any change is applied, so the transaction can simply continue or be rolled back.

## Supported transparently

These pass through with no extra steps. Where pg_deltax tracks the column by name, the catalog is updated automatically.

All of these operations are fast, none of them touch compressed data, so the cost is independent of how much data the table holds. Most are pure PostgreSQL catalog updates (constant-time per ALTER); a few (`RENAME COLUMN`, `DROP COLUMN`, `OWNER TO`, `GRANT` / `REVOKE`) do a small per-partition catalog update on pg_deltax's side, so they scale with the number of partitions but never with row count. `CREATE INDEX` is the exception: on uncompressed partitions it has the standard PG cost of scanning the heap; on compressed partitions the heap is empty so it's effectively a no-op.

| Operation | Notes |
|---|---|
| `ADD COLUMN` nullable, no default | Reads return NULL for partitions that were compressed before the column existed. |
| `ADD COLUMN DEFAULT <nonvolatile>` | Reads return the default for partitions compressed before the column existed (via PG's fast-default machinery). `nonvolatile` means `IMMUTABLE` or `STABLE`. |
| `ADD COLUMN NOT NULL DEFAULT <nonvolatile>` | Same as above. |
| `RENAME COLUMN` | Storage is positional, so nothing in compressed data moves. `time_column`, `segment_by`, `order_by`, and per-column compression metadata are renamed to match. |
| `RENAME TO` (rename the table) | Catalog updated. Partition table names don't auto-rename (standard PG behavior). |
| `SET SCHEMA` | Catalog updated. Partitions stay where they were (standard PG behavior). |
| `ALTER COLUMN SET/DROP DEFAULT` | Affects only future inserts. |
| `ALTER COLUMN DROP NOT NULL` | Relaxing a constraint; no impact on stored data. |
| `ALTER COLUMN SET STATISTICS` | Planner hint only. |
| `ADD CONSTRAINT … CHECK … NOT VALID` | No validation of existing data; applies to future inserts. |
| `ADD CONSTRAINT … FOREIGN KEY … NOT VALID` | Same. |
| `DROP CONSTRAINT` | — |
| `COMMENT ON …` | — |
| `OWNER TO`, `GRANT` / `REVOKE` | Cascaded to companion tables in `_deltax_compressed` so the new owner / grantee can still read compressed data. |
| `ENABLE/DISABLE TRIGGER` | Pass-through, except pg_deltax's own `deltax_reject_compressed_dml` trigger and `DISABLE TRIGGER ALL` are blocked. |
| `SET/RESET (storage_parameter)` | E.g. `fillfactor`, `autovacuum_*`. |
| `REPLICA IDENTITY …` | — |
| `CREATE INDEX` / `DROP INDEX` on the parent, non-unique only | Applies to current/future uncompressed partitions. Compressed custom scans use their own statistics and won't pick up these heap indexes. |
| `DROP COLUMN` (non-key column) | The column is tombstoned in pg_deltax's per-partition descriptor; reads stop returning it. The underlying compressed bytes for that column remain until the next time the partition is recompressed. |

"Non-key column" above means a column that is **not** the time column, in `segment_by`, or in `order_by`. Dropping a key column is blocked — see below.

## Blocked

These raise an error. The fix is the decompress-recompress recipe in the next section.

| Operation | Why |
|---|---|
| `ALTER COLUMN TYPE …` | Compressed blobs are codec-encoded per type and can't be reinterpreted in place. |
| `ALTER COLUMN SET STORAGE` / `SET COMPRESSION` | Would apply only to future rows, not to existing compressed storage. |
| `DROP COLUMN` for the time column, `segment_by`, or `order_by` | Compressed metadata is laid out around these columns by name. |
| `ALTER COLUMN SET NOT NULL` | PG would validate against the (empty) partition heap and pass falsely. |
| `VALIDATE CONSTRAINT` (after `ADD … NOT VALID`) | Same — validates against empty heaps. |
| `ADD CHECK` validating form | Requires scanning existing data. |
| `ADD PRIMARY KEY`, `ADD UNIQUE`, `ADD EXCLUDE`, `CREATE UNIQUE INDEX`, `ADD FOREIGN KEY` validating form | All require scanning existing data or proving cross-partition uniqueness. |
| `ADD COLUMN … DEFAULT <volatile>` (e.g. `random()`, `clock_timestamp()`, `nextval(...)`) | PG normally evaluates the default per existing row; compressed heaps are empty so the rewrite would silently be a no-op. |
| `ADD COLUMN NOT NULL` without a default | Same false-validation problem. |
| `ADD COLUMN … GENERATED`, `ALTER COLUMN SET/DROP EXPRESSION` | Generated values would normally be computed for existing rows. |
| `ADD/DROP IDENTITY`, `SET GENERATED` | Identity semantics depend on per-row writes. |
| `ALTER CONSTRAINT` | — |
| `ENABLE/DISABLE RLS`, `FORCE/NO FORCE RLS`, `CREATE/ALTER/DROP POLICY` | Parent-level RLS isn't yet wired through compressed scans. |
| `ENABLE/DISABLE RULE` | Rules can rewrite DML in ways the compressed scan path doesn't model. |
| `CLUSTER ON`, `SET WITHOUT CLUSTER` | Heap clustering doesn't describe compressed storage. |
| `SET ACCESS METHOD`, `SET TABLESPACE` | Don't move companion tables consistently. |
| `ALTER TABLE … OF` / `NOT OF` | Typed-table metadata isn't modeled. |
| `ATTACH PARTITION` / `DETACH PARTITION` | pg_deltax owns partition lifecycle; manual attach/detach would desync the catalog and background worker. |
| `INHERIT` / `NO INHERIT` | Doesn't apply to declarative partitions. |
| `DISABLE TRIGGER deltax_reject_compressed_dml`, `DISABLE TRIGGER ALL` | Would let DML through to compressed partitions. |

## The decompress → ALTER → recompress recipe

For any blocked operation, decompress the affected partitions, run the ALTER, then recompress.

```sql
-- 1. Identify compressed partitions.
SELECT schema_name || '.' || table_name AS partition
FROM deltax.deltax_partition p
JOIN deltax.deltax_deltatable d ON d.id = p.deltatable_id
WHERE d.schema_name = '<schema>' AND d.table_name = '<table>'
  AND p.is_compressed;

-- 2. Decompress each one.
SELECT deltax.deltax_decompress_partition('<schema>.<partition_name>');

-- 3. Run the schema change.
ALTER TABLE <schema>.<table> ALTER COLUMN <col> TYPE bigint;

-- 4. Recompress each partition.
SELECT deltax.deltax_compress_partition('<schema>.<partition_name>');
```

### Things to plan for

- The rewrite cost is O(total raw data) plus codec cost. On large tables, expect the operation to take a while.
- Peak on-disk usage during the recipe ≈ raw + compressed size for the partitions that are simultaneously decompressed.
- Pause the automatic compression policy for the table (`deltax.deltax_remove_compression_policy`, or just don't have one set) so the background worker doesn't recompress a partition mid-recipe.
- Concurrent writes during the recipe land in the current open partition normally. Most blocked ALTERs take `AccessExclusiveLock` on the parent, which already serializes writes.

### Per-partition variant

If the table is large enough that decompressing it whole is impractical, validation-only ALTERs (`SET NOT NULL`, `VALIDATE CONSTRAINT`, `ADD CHECK` validating, single-partition uniqueness) can be done one partition at a time:

```sql
-- For each compressed partition:
SELECT deltax.deltax_decompress_partition('<schema>.<partition>');
-- Verify the constraint holds for this partition's data, e.g.:
SELECT count(*) FROM <schema>.<partition> WHERE <col> IS NULL;  -- should be 0
SELECT deltax.deltax_compress_partition('<schema>.<partition>');

-- Once all partitions pass:
ALTER TABLE <schema>.<table> ALTER COLUMN <col> SET NOT NULL;
```

The final ALTER passes PG's validation trivially because the heaps are empty post-recompression, but the per-partition checks have already proven the constraint.

`ALTER COLUMN TYPE` and cross-partition uniqueness constraints (`ADD PRIMARY KEY` / `ADD UNIQUE`) cannot be done one partition at a time without taking the affected partitions offline; use the whole-table recipe.

## Not covered by ALTER TABLE

These are pg_deltax-specific configuration knobs, not schema changes — use the dedicated functions in [FUNCTIONS.md](FUNCTIONS.md):

- `partition_interval`, `compress_after`, `drop_after`
- `segment_by`, `order_by`, `segment_size`
- `json_extract` paths
