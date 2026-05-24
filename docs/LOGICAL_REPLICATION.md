# Logical replication

pg_deltax-managed tables are native PostgreSQL declaratively-partitioned tables, so they work with built-in logical replication (`CREATE PUBLICATION` / `CREATE SUBSCRIPTION`) without any extension-specific tooling. Two deployment models are possible; **Scenario 1 is recommended**.

## Scenario 1 — replicate the user table, replica compresses independently (recommended)

The publisher and the subscriber each manage their own partitions and compression. Logical replication only carries user-level INSERT/UPDATE/DELETE on the partitioned table. Each side decides when to compress and runs its own background worker.

```
           Publisher                              Subscriber

   metrics (partitioned)               metrics (partitioned)
   ├─ p20250115  [compressed]          ├─ p20250115  [heap]
   ├─ p20250116  [compressed]          ├─ p20250116  [compressed]
   ├─ p20250117  [heap]   ──INSERT───> ├─ p20250117  [heap]
   └─ p20250118  [heap]     UPDATE     └─ p20250118  [heap]
                            DELETE
   _deltax_compressed.*                _deltax_compressed.*
   (local companion tables)            (local, independent)

   deltax_partition catalog            deltax_partition catalog
   (local)                             (local)

   worker: compresses                  worker: compresses
   on its own schedule                 on its own schedule

   ╌╌╌╌╌╌╌╌╌╌╌╌╌╌  logical WAL stream  ╌╌╌╌╌╌╌╌╌╌╌╌╌╌
   only carries DML on metrics; _deltax_compressed and
   deltax_partition stay local to each side
```

This is recommended because:

- One-time schema bootstrap is all that's needed.
- The publisher's compression is invisible to the subscriber — companion tables, the `_deltax_compressed` schema, and `deltax_partition` catalog rows stay local to each side.
- New partitions created by the worker on either side are independent. With `publish_via_partition_root = true`, leaves attached to the parent on the publisher are automatically published.
- The subscriber can run a different compression schedule (e.g. compress aggressively for storage, or defer to keep recent data uncompressed for ad-hoc edits).

### Setup

On both publisher and subscriber, set `wal_level = logical` (publisher minimum; harmless on subscriber) and install pg_deltax:

```sql
ALTER SYSTEM SET wal_level = 'logical';
-- restart postgres
CREATE EXTENSION pg_deltax;
```

Copy the schema once (standard logical replication practice — DDL is never carried by logical decoding):

```bash
pg_dump --schema-only --no-publications --no-subscriptions \
  -d source_db | psql -d target_db
```

On the **publisher**, create the publication. Two settings matter:

```sql
CREATE PUBLICATION my_pub
  FOR TABLE my_metrics
  WITH (
    publish_via_partition_root = true,
    publish = 'insert, update, delete'
  );
```

- `publish_via_partition_root = true` makes the subscriber receive changes as if they applied to the root, so its own partition routing kicks in independently — no partition-name coupling between the two sides, and any future leaf partitions on the publisher are auto-included.
- `publish = 'insert, update, delete'` **excludes TRUNCATE**. This is critical: pg_deltax's compression flow runs `TRUNCATE <leaf_partition>` after copying rows into the companion tables, and a replicated TRUNCATE would wipe the subscriber's rows for any partition the subscriber hasn't yet compressed locally.

On the **subscriber**, create the subscription:

```sql
CREATE SUBSCRIPTION my_sub
  CONNECTION 'host=publisher-host port=5432 user=replicator password=... dbname=source_db'
  PUBLICATION my_pub;
```

### REPLICA IDENTITY for UPDATE/DELETE

pg_deltax-managed tables generally have no primary key (time-series append workloads don't need one). UPDATE/DELETE replication requires a replica identity. Two options:

```sql
-- Option A: REPLICA IDENTITY FULL on every leaf partition.
ALTER TABLE my_metrics REPLICA IDENTITY FULL;
-- Plus every existing and future leaf partition:
ALTER TABLE my_metrics_p20250115 REPLICA IDENTITY FULL;
-- ...

-- Option B: add a unique index that propagates with partition definitions.
CREATE UNIQUE INDEX my_metrics_uniq ON my_metrics (ts, device_id);
ALTER TABLE my_metrics REPLICA IDENTITY USING INDEX my_metrics_uniq;
```

REPLICA IDENTITY is checked at the **leaf-partition** storage level — setting it only on the parent is silently ignored for UPDATE/DELETE. Because the background worker creates new leaf partitions on its 60-second tick, INSERT-only workloads need no action, but UPDATE/DELETE workloads need to ensure new partitions inherit the identity. A unique index (Option B) is the simplest way: it's automatically copied to each new partition created with `CREATE TABLE ... PARTITION OF`.

### Caveats

- **Do not include TRUNCATE in `publish`.** See above.
- **REPLICA IDENTITY does not propagate from parent to leaves.** Use a unique index, or set it on each leaf.
- The subscriber's worker also drains its own default partition and compresses on its own schedule. There is no coordination between the two — they each see a logically-equivalent dataset and converge on storage layouts independently.

## Scenario 2 — replicate companion tables, query-only replica

In this model the subscriber never runs compression — it just receives the compressed bytes (companion tables) and uses pg_deltax only for the query/decompression path. The compressed catalog row, the companion tables, and the partition heap all need to stay in sync between the two sides.

```
           Publisher                              Subscriber

   metrics (partitioned)               metrics (partitioned)
   ├─ p20250115  [compressed]          ├─ p20250115  [compressed]
   ├─ p20250116  [compressed]          ├─ p20250116  [compressed]
   └─ p20250117  [heap]    ──INSERT──> └─ p20250117  [heap]

   _deltax_compressed.*    ──COPY────> _deltax_compressed.*
   (companion tables)                  (refilled by replication)

   deltax_partition        ──COPY────> deltax_partition
   (catalog)                           (refilled by replication)

   worker: compresses                  worker: disabled
                                       (no local compression)

   ╌╌╌╌╌╌╌╌╌╌╌╌╌╌  logical WAL stream  ╌╌╌╌╌╌╌╌╌╌╌╌╌╌
   carries DML on metrics AND on _deltax_compressed.*
   AND on deltax_partition; subscriber is read-only
```

Logical replication is structurally awkward in this mode because:

- pg_deltax creates companion tables (`_deltax_compressed.<partition>_meta`, `_blobs`, `_colstats`, ...) on demand when a partition gets compressed.
- Logical replication never replicates DDL, so the subscriber would receive INSERTs into companion tables it has never seen, and the apply worker would error.
- The DML-reject trigger that compression installs on the leaf partition is also DDL — it doesn't replicate either.

Workable approaches:

1. **DDL replication**: this can be done, for example, using pgstream.
2. **A pg_deltax-side affordance** (not implemented today): have the catalog row act as the source of truth and materialize companion-table shells on the subscriber when a new `deltax_partition` row arrives via replication.

Once the companion tables exist on the subscriber with matching schemas, a publication scoped to `_deltax_compressed.*` (plus `deltax_partition`) does move the compressed bytes correctly.
