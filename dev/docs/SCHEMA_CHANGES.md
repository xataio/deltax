# Schema Changes — Plan

## Status

**Shipped on `schema_changes` branch.** Implementation summary:

- **Descriptor catalog** — `compressed_columns` JSONB on `deltax.deltax_partition`, snapshotted at compression time (`catalog::snapshot_compressed_columns`). The scan path's `MetadataInfo.blob_idx` reads it back via `scan::exec::segments::load_metadata`, with missing-value synthesis via `pg_sys::getmissingattr` for columns added after compression. Legacy partitions whose descriptor is NULL fall back to positional `pg_attribute` mapping, bit-for-bit unchanged from the pre-feature behavior.
- **ProcessUtility-hook ALTER dispatch** — `src/ddl.rs` classifies every `T_AlterTableStmt` / `T_RenameStmt` / `T_AlterObjectSchemaStmt` against the tier matrix below. Tier 1 ops pass through and run optional catalog bookkeeping (RENAME COLUMN updates `segment_by` / `order_by` / `time_column` and JSONB keys in `column_ndistinct` / `column_valmap` / `compressed_columns`). Tier 2 `DROP COLUMN` for a non-key column passes through and flips `dropped: true` on every matching descriptor entry. Tier 3 ops `ereport(ERROR, ERRCODE_FEATURE_NOT_SUPPORTED)` with a `HINT` pointing at the recipe below. The block is gated on `has_compressed_partitions` so the recipe itself just works. A thread-local bypass (`ddl::with_bypass`) lets pg_deltax's own internal DDL (partition rotation, COPY finalization, `CREATE EXTENSION` migrations) sidestep the hook.
- **Safety guards** — `DISABLE TRIGGER deltax_reject_compressed_dml` and `DISABLE TRIGGER ALL` are Tier 3 (would silently let DML through to compressed partitions). DROP of a key column (`segment_by` / `order_by` / `time_column`) is Tier 3 because `_meta` embeds those by name.
- **Companion-table cascade** — `OWNER TO new_owner` on a deltatable cascades onto every companion table in `_deltax_compressed.*` so the parent and its `_meta` / `_blobs` / `_colstats` / `_blooms` / `_text_lengths` / `_valbitmap` companions stay owned by the same role. `GRANT` / `REVOKE` on the parent likewise cascades the same privilege change onto every companion. Column-level grants pass through unchanged (companion tables use renamed internal columns, so user-facing column names don't apply).
- **Aggregation + filter pushdown on added columns** — the agg paths in `scan/exec/agg/{compact,mixed,serial,metadata}.rs` consult `MetadataInfo.blob_idx` and synthesize from `MetadataInfo.missing_values` for columns added after compression, mirroring the basic decompress path. `SELECT count(*) WHERE new_col = X`, `sum(new_col)`, `GROUP BY new_col` all work correctly.

## Goal

For ALTER operations we can apply efficiently to already-compressed partitions, support them **transparently** — no user action beyond the ALTER itself. For everything else, **block** the operation with a clear error message and a documented decompress-then-recompress recipe.

## Why most column-shape ALTERs are cheap for us

After compression, a partition's data lives in companion tables in the `_deltax_compressed` schema (`_meta`, `_blobs`, `_colstats`, `_blooms`, `_text_lengths`, `_valbitmap`). Most of their schemas are **fixed**: `(_col_idx, _segment_id, …)`. Source columns map to companion-table **rows keyed by `_col_idx`**, not to companion-table columns. So adding, renaming, or even dropping many source columns normally **does not require any DDL on companion tables** — only a change in how we interpret existing blobs at decompress time.

Two caveats drive the blocking rules:

- `_col_idx` currently means the compression-time ordinal of **non-`segment_by`** columns, not the physical table attnum.
- `_meta` is not fully fixed: it embeds `segment_by` columns and time-column min/max columns by name/type.

There's already a precedent in the codebase: synthetic JSON-extracted columns added after a partition was compressed are silently skipped at decompress (`json_extract_added_at` mechanism). We generalize the same trick to physical columns.

The catalog (`deltax_deltatable`, `deltax_partition`) stores some column references by name (`time_column`, `segment_by`, `order_by`, `column_ndistinct`, `column_valmap`). Those need to stay in sync on `RENAME COLUMN`.

This plan intentionally starts conservative: support only schema changes whose semantics are clear for compressed custom scans, and block anything that would require validating, rewriting, or reinterpreting existing compressed rows.

## Operation classification

### Tier 1 — Supported transparently (no blob touch)

These pass straight through. Where the catalog references columns by name, we update it.

| Operation | Notes |
|---|---|
| `ADD COLUMN` nullable, no default | Decompress synthesizes NULL for partitions compressed before the column existed. |
| `ADD COLUMN DEFAULT <nonvolatile>` | Here "nonvolatile" means immutable or stable. Decompress reads `pg_attribute.attmissingval` for partitions compressed before the column existed. PG's own fast-default machinery already populates `attmissingval`, so we reuse it. |
| `ADD COLUMN NOT NULL DEFAULT <nonvolatile>` | Same as above. Safe because every existing row has the same missing value. |
| `RENAME COLUMN a TO b` | Storage is positional; nothing in companions changes. Update `time_column`, `segment_by`, `order_by`, `deltax_partition.column_ndistinct`, `deltax_partition.column_valmap`, and `compressed_columns`/column-descriptor metadata if they contained `a`. |
| `RENAME TO` (rename the table) | Update `deltax_deltatable.table_name`. Partition table names don't auto-rename in PG; companion-table names stay tied to the partition name; nothing else moves. |
| `SET SCHEMA` | Update `deltax_deltatable.schema_name`. Partitions and companions stay where they were (PG behavior). |
| `ALTER COLUMN SET/DROP DEFAULT` | Affects only new inserts. Pass through. |
| `ALTER COLUMN DROP NOT NULL` | Relaxing a constraint; no impact on stored blobs. Pass through. |
| `ALTER COLUMN SET STATISTICS` | Planner hint only. Pass through. |
| `ADD CONSTRAINT ... CHECK ... NOT VALID` | No validation of existing data; affects future inserts. Pass through. |
| `ADD CONSTRAINT ... FOREIGN KEY ... NOT VALID` | Same. Pass through. The validating form lives in Tier 3. |
| `DROP CONSTRAINT` | Pass through. |
| `COMMENT ON ...` | Pass through. |
| `OWNER TO`, `GRANT` / `REVOKE` | Pass through. Should also cascade to companion tables so they don't become inaccessible to the new owner. |
| `ENABLE/DISABLE TRIGGER` | Pass through, but do not allow disabling pg_deltax's own compressed-partition DML rejection trigger. |
| `SET/RESET (storage_parameter)` (fillfactor, autovacuum_*, etc.) | Pass through; irrelevant once data is compressed away. |
| `REPLICA IDENTITY ...` | Pass through. |
| `CREATE INDEX` / `DROP INDEX` on the parent, non-unique only | Pass through. The index applies to the current/future uncompressed partitions and is a no-op on compressed partitions. Document that compressed custom scans will not use these child heap indexes. |

### Tier 2 — Supported with light catalog bookkeeping

| Operation | Notes |
|---|---|
| `DROP COLUMN` for a non-`segment_by`, non-`order_by`, non-time column | Mark the compression-time column descriptor tombstoned in `deltax_partition.compressed_columns`. Decompress skips that `_col_idx`. Orphan rows in `_blobs/_colstats/etc` stay until the next recompression of that partition (or a future GC). Requires storing a per-partition column descriptor list, not just a count. |

### Tier 3 — Blocked in v1, with documented recipe

All raise an error from the utility hook, naming the operation and pointing at the decompress-recompress recipe below.

| Operation | Why blocked |
|---|---|
| `ALTER COLUMN TYPE ...` | Blobs are codec-encoded per type; cannot be reinterpreted in place. |
| `ALTER COLUMN SET STORAGE` / `ALTER COLUMN SET COMPRESSION` | Split semantics: affects future heap/TOAST rows but not existing compressed blobs. Block until explicitly supported/documented. |
| Adding/dropping/renaming a column referenced by `segment_by` if the `_meta` schema cannot be kept consistent | `_meta` table schema embeds segment_by columns. Simple rename is Tier 1 only if companion metadata and planner paths are updated correctly. |
| Adding/dropping/renaming a column referenced by `order_by` if planner paths cannot be kept consistent | Blobs are encoded with the existing sort/run-length structure. Simple rename is Tier 1 only if metadata and sort-key lookups remain correct. |
| `DROP COLUMN` for the time column | `_meta` stores time-column min/max by name and the partitioning scheme depends on it. |
| `ALTER COLUMN SET NOT NULL` without a nonvolatile default supplied by `ADD COLUMN` | PG would validate against the (empty) partition table and pass trivially. We don't want to silently lie. Block until a "validate against compressed data" path exists. |
| `VALIDATE CONSTRAINT` (after `ADD ... NOT VALID`) | Same problem — PG scan against empty partitions validates falsely. |
| `ADD CHECK` validating form | Requires scanning existing data. |
| `ADD PRIMARY KEY`, `ADD UNIQUE`, `ADD EXCLUDE`, `CREATE UNIQUE INDEX`, `ADD FOREIGN KEY` (validating form) | All require scanning existing data or proving cross-partition uniqueness/referential integrity. |
| `ADD COLUMN ... DEFAULT <volatile>` (e.g. `random()`, `clock_timestamp()`, `nextval(...)`) | PG normally rewrites every row evaluating the default per-row. Our heap is empty, so PG's rewrite is a silent no-op — semantics would diverge. Block; force users through the recipe. |
| `ADD COLUMN NOT NULL` without a default | Would validate falsely against empty compressed heaps. |
| `ADD COLUMN ... GENERATED`, `ALTER COLUMN SET EXPRESSION`, `ALTER COLUMN DROP EXPRESSION` | Stored generated values would normally be computed/recomputed for existing rows. Block until compressed-data rewrite exists. |
| `ADD/DROP IDENTITY`, `SET GENERATED`, identity sequence option changes | Identity semantics are tied to per-row writes and sequence behavior; keep out of v1. |
| `ALTER CONSTRAINT` | Constraint semantics vary by constraint type; keep blocked until each subtype is classified. |
| `ENABLE/DISABLE RLS`, `FORCE/NO FORCE RLS`, `CREATE/ALTER/DROP POLICY` | Custom scans read companion tables; block until tests prove parent RLS policies are enforced identically for compressed data. |
| `ENABLE/DISABLE RULE` | Rules can rewrite DML/query behavior in ways the compressed scan path does not model. |
| `CLUSTER ON`, `SET WITHOUT CLUSTER` | Heap ordering metadata does not describe compressed storage. |
| `SET ACCESS METHOD`, `SET TABLESPACE` | Rewrite/storage-placement operations do not move companion tables consistently. |
| `ALTER TABLE ... OF` / `NOT OF` | Typed-table metadata is outside v1. |
| `ATTACH PARTITION` / `DETACH PARTITION` | pg_deltax owns partition lifecycle; manual attach/detach would desync the worker and catalog. |
| `INHERIT` / `NO INHERIT` | Defensive block — doesn't apply to declarative partitions. |

### Out of scope for this work

These are pg_deltax-specific configuration changes, not `ALTER TABLE` operations, and need dedicated `deltax_*` functions:

- Changing `partition_interval`, `compress_after`, `drop_after`
- Changing `segment_by`, `order_by`, `segment_size`
- Adding/removing `json_extract` paths (`json_extract_added_at` already covers the live case)

## The decompress → ALTER → recompress recipe

For any Tier 3 operation, users follow this on each affected table. Suitable for small-to-medium tables; for very large ones, plan for the rewrite time and disk-space overhead (peak ≈ raw + compressed size during the operation).

```sql
-- 1. Identify compressed partitions.
SELECT schema_name || '.' || table_name AS partition
FROM deltax_partition p
JOIN deltax_deltatable d ON d.id = p.deltatable_id
WHERE d.schema_name = '<schema>' AND d.table_name = '<table>'
  AND p.is_compressed;

-- 2. Decompress each one.
SELECT deltax_decompress_partition('<schema>.<partition_name>');

-- 3. Run the schema change.
ALTER TABLE <schema>.<table> ALTER COLUMN <col> TYPE bigint;

-- 4. Recompress each partition.
SELECT deltax_compress_partition('<schema>.<partition_name>');
```

For a whole-table convenience, `deltax_decompress_table(...)` / `deltax_compress_table(...)` wrappers may be added later.

**Caveats:**

- Rewriting all partitions is O(total raw data) plus codec cost. Plan for it on large tables.
- During the window where partitions are decompressed, queries pay heap-scan cost on those partitions and disk usage grows.
- Disable or pause pg_deltax compression policy/workers for the table before starting the recipe, or hold a table-level maintenance lock, so a background pass cannot recompress a partition mid-recipe.
- Concurrent writes during the recipe land in the current (uncompressed) partition normally — no special handling needed unless the ALTER itself blocks the parent. Most ALTERs in Tier 3 take `AccessExclusiveLock` on the parent, which already serializes writes.

### Per-partition variant (bounded disk)

The recipe above decompresses every partition at once before the ALTER, which means peak disk ≈ sum of raw sizes for the whole table. On large tables that's a problem. Whether a per-partition variant is feasible depends on the operation:

**Validation-only Tier 3 ops** (`SET NOT NULL`, `VALIDATE CONSTRAINT`, `ADD CHECK` validating, single-partition uniqueness): cleanly per-partition.

```sql
-- For each compressed partition:
SELECT deltax_decompress_partition('<schema>.<partition>');
-- Verify the constraint holds for this partition's data
SELECT count(*) FROM <schema>.<partition> WHERE <col> IS NULL;  -- should be 0
SELECT deltax_compress_partition('<schema>.<partition>');
-- After all partitions pass:
ALTER TABLE <schema>.<table> ALTER COLUMN <col> SET NOT NULL;
```

The final ALTER trivially passes PG's validation because partitions are empty post-recompression, but the user-run per-partition check has already proven the constraint. Peak disk ≈ one partition's raw size.

**`ALTER COLUMN TYPE`**: PG's partition-schema rule (every partition's schema must match the parent at all times) makes the clean per-partition recipe above impossible — you can't have one partition holding `BIGINT` while the parent still says `INT`. Two workarounds, both messier:

1. **DETACH / re-ALTER / re-ATTACH per partition.** Detach P, decompress, ALTER P standalone, recompress, leave detached. Repeat for all partitions. Then ALTER parent (now empty), then re-attach. Bounded disk, but the table is partially functional for the duration (queries miss detached partitions, writes only land in attached ones).
2. **In-place blob re-encoding.** Decode old blobs, cast to new type, encode as new blobs — all inside `_deltax_compressed`, without involving PG's heap. Run per partition, then final ALTER on parent is metadata-only since heaps are empty. Bounded disk and no DETACH, but requires implementing codec-to-codec conversion. Worth building only if disk pressure on TYPE changes turns out to bite real users.

**`ADD PRIMARY KEY` / `ADD UNIQUE`**: inherently cross-partition — uniqueness can't be proven one partition at a time without a global structure. Per-partition decompress doesn't help; either keep the whole-table recipe, or build a cross-partition validator (e.g. ndistinct sketches per partition + cross-check).

## Implementation outline

### Interception

Use a `ProcessUtility_hook`, not a plain SQL event trigger, for v1.

Reason: we need exact `AlterTableStmt` / `AlterTableCmd` subcommand classification before PostgreSQL rewrites or validates empty compressed heaps. SQL event triggers are useful for coarse DDL observation, but `ddl_command_end` is post-facto and `pg_event_trigger_ddl_commands()` does not give the same direct, preflight control over every ALTER subcommand. A utility hook lets us inspect the parse tree, reject unsafe operations before execution, and pass safe operations through to the previous hook / `standard_ProcessUtility`.

The hook itself is already installed by `copy::register_process_utility_hook()` from `_PG_init` to intercept `COPY ... FORMAT deltax_compress` and `ANALYZE`. The ALTER work **extends** that existing dispatcher — we don't install a second hook. The new module `src/ddl.rs` owns the `AlterTableStmt` / `RenameStmt` / `AlterObjectSchemaStmt` walkers; the existing hook calls into it when the target relation belongs to a registered deltatable.

Steps:

1. Resolve the target table; check whether it (or its parent for partitions) is registered in `deltax_deltatable`. If not, pass through.
2. Walk the `AlterTableStmt` subcommands. Each subcommand maps to one of the tiers above.
3. Tier 1: optionally update catalog (RENAME COLUMN / RENAME TO / SET SCHEMA, GRANT cascade). Otherwise nothing.
4. Tier 2 (`DROP COLUMN`): update `deltax_partition.compressed_columns` with a tombstone for that position.
5. Tier 3: `RAISE EXCEPTION` with a message naming the operation, the affected table, and the recipe.

### Catalog change

Extend `deltax_partition` with `compressed_columns JSONB` capturing, in order, the compression-time descriptors for source columns. Do not use a bare `TEXT[]`: names alone are ambiguous across rename/drop/type-change history, and `_col_idx` is not the same as physical attnum when `segment_by` columns are present.

Recommended shape:

```json
[
  {
    "attnum": 1,
    "name": "ts",
    "type_oid": 1184,
    "typmod": -1,
    "is_segment_by": false,
    "compressed_col_idx": 0,
    "dropped": false
  }
]
```

Rules:

- Descriptor present and not tombstoned, matching current `pg_attribute` type/typmod: emit decompressed values.
- Descriptor present but tombstoned or current `pg_attribute` marks the attnum dropped: skip the `_col_idx`.
- Current `pg_attribute` column absent from the partition's descriptors: synthesize from `attmissingval` or NULL.
- Descriptor type/typmod differs from current `pg_attribute`: error defensively. The utility hook should have blocked the DDL, but the scan path must not silently decode old blobs as a new type.

A minimal alternative is `compressed_col_count INT`, which supports Tier 1 ADD COLUMN only. Recommend the full column list so Tier 2 is unlocked with the same shape.

The descriptor snapshot must be written anywhere a partition becomes compressed: normal `deltax_compress_partition`, direct backfill/COPY finalization, and any future bulk-load path.

### Files likely involved

- `src/lib.rs` — extend `deltax_partition` schema.
- `src/catalog.rs` — `PartitionInfo`, register/lookup helpers, tombstone helper, RENAME / SET SCHEMA updaters.
- `src/compress.rs` — snapshot the current column descriptors into `compressed_columns` when compressing.
- `src/copy.rs` — write the same snapshot for direct backfill/COPY finalization paths.
- `src/scan/exec/segments.rs` — `MetadataInfo`, `load_metadata` consult `compressed_columns`, drop tombstoned positions.
- `src/scan/exec/datum_utils.rs` — missing-blob synthesis: read `attmissingval`, else NULL.
- New file `src/ddl.rs` — utility-hook handler walking `AlterTableStmt`.
- `src/lib.rs` — install/uninstall the utility hook from `_PG_init` / `_PG_fini`.
- `tests/test_schema_changes.py` — new integration test file covering each Tier 1/2/3 operation against both uncompressed and compressed partitions.

## Test plan

Per-operation tests, each parametrized over the partition state (uncompressed, compressed, mixed):

- Tier 1: run the ALTER, verify the data reads correctly before/after and the catalog reflects any rename.
- Tier 2 (DROP COLUMN): run the ALTER, verify reads return the right shape, verify tombstone is recorded, verify a later recompression GCs the orphan rows.
- Tier 3: assert the ALTER errors with the expected message, then run the documented recipe and assert it succeeds.

## Future work

- Auto-running the decompress/recompress recipe in the background for Tier 3 operations (move from "block" to "do it for you").
- A `deltax_alter_table(...)` convenience wrapper that performs the recipe atomically per partition.
- GC of orphan blob rows after DROP COLUMN (right now they linger until the next recompression).
- Per-partition `SET NOT NULL` validation against compressed data (would lift one Tier 3 entry).
