# Loading pg_deltax: `shared_preload_libraries` vs `session_preload_libraries`

pg_deltax installs PostgreSQL planner and executor hooks when its shared library
is loaded into a backend. Those hooks are what make reads of compressed data
transparent: after a partition is compressed, its rows move into columnar
companion tables and the original heap is emptied, and a custom scan
reconstructs the rows on `SELECT`. **A backend that does not have pg_deltax
loaded cannot see that data** — it scans the empty heap and returns nothing for
compressed partitions.

There are two ways to get the library loaded into your backends:

- **`shared_preload_libraries`** The postmaster loads pg_deltax once 
  at server startup and every backend inherits it. Requires a one-time server restart to enable.
- **`session_preload_libraries`** Each backend loads pg_deltax at
  connection time. No restart, it can be scoped to a single database, and a
  database owner can enable it without cluster-level configuration access.

## Comparison

| | Full mode — `shared_preload_libraries` | Session mode — `session_preload_libraries` |
|---|---|---|
| Restart to enable | Yes (one-time) | No |
| Who can enable it | Cluster admin (server config) | Database owner (`ALTER DATABASE`) |
| Scope | Whole cluster (all databases) | A chosen database (or role) |
| Footprint on databases that don't use pg_deltax | Hooks loaded into every backend cluster-wide — a small per-query check runs even in databases that never `CREATE EXTENSION pg_deltax` | None — loaded only where you enable it |
| Transparent reads of compressed data | ✅ every backend | ✅ in backends that load the library |
| Automatic background maintenance | ✅ built-in worker | ❌ you schedule `deltax_run_maintenance()` (e.g. with pg_cron) |
| Shared blob cache (cold-read speedup) | ✅ | ❌ off (performance only, not correctness) |
| Safe if a client connects without the library | ✅ can't happen | ⚠️ that backend silently returns **zero rows** from compressed partitions — see [Caveats](#caveats) |
| Per-connection cost | None | A tiny `dlopen` at connect (negligible, especially behind a pooler) |

## Full mode

Full mode is generally easier to use and is recommended if the Postgres instance is dedicated
to the DeltaX use-case. For example, if it has only one database and that has DeltaX loaded.

Add pg_deltax to `shared_preload_libraries` and restart, then create the
extension in each database that needs it:

```sh
# postgresql.conf — needs a one-time restart
echo "shared_preload_libraries = 'pg_deltax'" >> $PGDATA/postgresql.conf
# restart PostgreSQL, then:
psql -c "CREATE EXTENSION pg_deltax;"
```

That's all that's required: the background maintenance worker starts
automatically, the shared blob cache is available, and every connection — your
application, `pg_dump`, replication, ad-hoc `psql` — has the hooks. See the
[Configuration reference](CONFIGURATION.md) for the worker and blob-cache GUCs.

## Session mode

Session mode is generally recommended if you use a Postgres instances for multiple
databases/use-cases and not all of them require DeltaX. This is because the shared lib
can be loaded only for the databases that actually need it. And it might be required
in cases where full Postgres server restarts are inconvenient for various reasons.

### Enable it

As the owner of the database that holds your deltatables:

```sql
-- 1. Create the catalog and SQL functions (one-time, per database).
CREATE EXTENSION pg_deltax;

-- 2. Load the library on every new connection to this database — no restart.
ALTER DATABASE analytics SET session_preload_libraries = 'pg_deltax';
```

New connections to `analytics` now load pg_deltax automatically and read
compressed data transparently. (Existing connections keep their current state;
reconnect to pick it up.)

Scope it at the **database** level as shown, not per role. A role-scoped setting
(`ALTER ROLE … SET session_preload_libraries`) only loads the library for that
role's connections, so a connection pooler authenticating as a different role —
or any tool that connects as another role — ends up without the hooks. See
[Caveats](#caveats).

To enable it cluster-wide without a restart (the closest equivalent to full
mode, minus the background worker and shared cache):

```sql
ALTER SYSTEM SET session_preload_libraries = 'pg_deltax';
SELECT pg_reload_conf();
```

### Run maintenance yourself

In full mode a background worker drains the default partition, pre-creates future
partitions, compresses eligible partitions, and applies retention roughly once a
minute. Session mode has **no such worker**, so you schedule maintenance
yourself by calling `deltax_run_maintenance()`. One call runs all of those steps
for every deltatable in the database it runs in:

```sql
SELECT deltax.deltax_run_maintenance();
```

The easiest way to run it on a schedule is [pg_cron](https://github.com/citusdata/pg_cron)
(itself a `shared_preload_libraries` extension — a one-time setup):

```sql
-- One job per database that uses pg_deltax, matching the built-in 60s cadence.
SELECT cron.schedule_in_database(
    job_name => 'pg_deltax-maintenance-analytics',
    schedule => '* * * * *',                          -- every minute
    command  => 'SELECT deltax.deltax_run_maintenance()',
    database => 'analytics'
);
```

Any scheduler works — a system cron job running
`psql -c 'SELECT deltax.deltax_run_maintenance()'` against each database is
enough. The job must run as a superuser or a role that can manage the deltatables
(create/drop partitions and write the `deltax` catalog). The function no-ops on a
replica and is safe to call even in full mode (it coordinates with the background
worker so the two never collide).

### Caveats

**A backend without the library silently skips compressed data.**  If a connection 
does not have pg_deltax loaded — because the setting was scoped to a different role, a tool
connects in a way that bypasses it, or the database was simply forgotten — then
`SELECT`s from a deltatable return **zero rows for every compressed partition**,
with no error. Recent, not-yet-compressed partitions still read normally, so it
can look as though only older data has gone missing. There is no runtime guard
against this; avoid it by configuration:

- Scope `session_preload_libraries` at the **database** level (as above) so every
  backend on the database loads the library regardless of which role connects.
- Make sure **backup, ETL, and replication** tooling connects to a database (or
  cluster) where pg_deltax is loaded. A `pg_dump` taken from a backend without
  the library is **silently incomplete** for compressed partitions.
- If you can't guarantee that every reader loads the library, use **full mode** —
  the postmaster loads it into every backend and this situation cannot arise.

**No shared blob cache.** The process-shared blob cache (a cold-read latency
optimization) is only set up at postmaster start, so it is off in session mode.
Correctness and warm-cache performance are unaffected; only cold reads of
compressed segments are a little slower. This is subject for improvement in the
future.

**Postmaster-only GUCs are unavailable.** `pg_deltax.target_database`,
`pg_deltax.blob_cache_mb`, and `pg_deltax.blob_cache_shards` are
postmaster-context settings tied to the background worker and shared cache. They
are not defined at all in session mode (`SHOW` reports an unrecognized
parameter); treat them as full-mode-only knobs. All other `pg_deltax.*` GUCs work
in both modes.
