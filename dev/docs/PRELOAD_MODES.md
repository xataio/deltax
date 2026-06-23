# Preload Modes: `shared_preload_libraries` vs `session_preload_libraries`

Status: design / proposal, **Phase 1 implemented**. The "Decided" section is
settled in principle, with two carve-outs that are still open: the *mechanism*
of the runtime guard (#3 — the guarantee is decided, the implementation is not)
and the session-mode background-worker scheduling (options listed below).

Phase 1 (shipped): the dual-mode `_PG_init` branch (#1), the factored
`deltax_run_maintenance()` SQL entry point with per-table subtransaction
isolation + replica guard + pass-level advisory-lock mutual exclusion, and the
worker reusing the same code path. **Not** yet implemented and required before
session mode is *safe to expose*: the runtime guard (#3). Until then session
mode can be loaded but a mis-scoped backend can still silently return zero rows
from compressed partitions.

## Motivation

Today pg_deltax effectively requires `shared_preload_libraries = 'pg_deltax'`.
That requirement is a real adoption barrier:

- Changing `shared_preload_libraries` needs a **server restart** — a reboot on
  managed Postgres, a rolling restart under operators like CloudNativePG.
- It loads our hooks into **every database in the cluster**, including ones
  that never ran `CREATE EXTENSION pg_deltax` (the root of the ProcessUtility
  issue handled by `catalog::catalog_present()`).
- Enabling it is a **postmaster-level operation**: it needs access to the
  server config (and a restart window), so a database owner can't turn pg_deltax
  on for their own database without involving whoever runs the cluster.

`session_preload_libraries` addresses all three: set via `ALTER DATABASE … SET
session_preload_libraries` it takes effect on **new connections with no
restart**, is **scoped** to the chosen database or role, and can be done by the
database owner without postmaster-level config access.

The catch is that `shared_preload_libraries` gives us three things, and only one
of them survives unchanged under `session_preload_libraries`:

| Capability | How it's wired today | Under `session_preload` |
| --- | --- | --- |
| Query hooks (custom scan, agg pushdown, ProcessUtility) — **correctness** | set in `_PG_init` (`scan::register_hook`, `scan::register_executor_start_hook`, `copy::register_process_utility_hook`) | ✅ identical — hooks are per-backend, installed at connection start |
| Background maintenance worker | static `RegisterBackgroundWorker` in `worker::register_bgworker` (launcher) | ❌ static registration is postmaster-only |
| Shared blob cache | `RequestAddinShmemSpace` via `blob_cache::register_hooks` | ❌ fixed shmem reservation is postmaster-only |

### Why this is a correctness issue, not just a feature toggle

After `deltax_compress_partition`, the original partition heap is **truncated**
(`compress.rs`, "Truncate original partition"); the rows live only in the
columnar companion tables, and the custom scan installed by
`scan::register_hook` (`set_rel_pathlist_hook`) is what reconstructs them. If a
backend does not have that hook installed, `SELECT * FROM <deltax table>` plans
a plain heap scan over an empty partition and **silently returns zero rows for
every compressed partition** (recent, still-uncompressed partitions return
normally — so it looks half-working, which is worse).

With `shared_preload_libraries` this can't happen: the postmaster loads the
library and every forked backend inherits the hooks. With
`session_preload_libraries` it *can* happen if the setting is mis-scoped (set
for one role but not another, a database forgotten, a tool connecting
differently). So offering this mode **requires** a guard that fails loudly
instead of returning partial data.

Concrete mis-scoping vector to design against: `session_preload_libraries` is
applied from the **database and role** settings at connection start. A
transaction-mode connection pooler (e.g. PgBouncer) that authenticates as a
different backend role than the one carrying `ALTER ROLE … SET
session_preload_libraries` will produce backends *without* the library — the
abstract "mis-scoped" warning is, in practice, mostly a role-vs-database scoping
mismatch. Prefer `ALTER DATABASE … SET` (or `ALTER SYSTEM SET`) over per-role
settings to avoid it.

Scope-narrowing observation (a point in the design's favor): the dangerous state
only exists *after compression has truncated a partition*. In session mode, if a
user never schedules maintenance, **no compression runs → nothing is truncated →
reads stay correct** (the default and uncompressed partitions are scanned
normally); the database is merely unmaintained (default partition grows, no
premake/retention). The silent-empty risk is therefore confined to: a partition
that *was* compressed, later read by a backend with no hooks. This bounds what
the guard must protect.

## Decided

### 1. One build, dual mode, branch on the preload flag

PostgreSQL exposes `process_shared_preload_libraries_in_progress`
(`pg_sys::process_shared_preload_libraries_in_progress`), true only while
`_PG_init` runs during postmaster-time shared-preload processing. `_PG_init`
branches on it:

```
_PG_init():
    define_gucs()                 # always
    install_query_hooks()         # always — scan + executor + ProcessUtility hooks
    mark_hooks_installed()        # always — sets the per-backend sentinel (see #3)

    if process_shared_preload_libraries_in_progress:
        worker::register_bgworker()      # static launcher; postmaster-only
        blob_cache::register_hooks()     # RequestAddinShmemSpace; postmaster-only
    else:
        # loaded via session_preload / LOAD / fmgr:
        #  - no static worker (see "Background worker" below)
        #  - blob cache stays off for now (see "Blob cache")
```

The precedent for installing hooks in both modes is `auto_explain`
(`contrib/auto_explain/auto_explain.c`): its `_PG_init` has no
`process_shared_preload_libraries_in_progress` check at all — it defines its
GUCs and installs all four executor hooks unconditionally, so it works
identically in either load mode. That is the shape pg_deltax wants for its query
hooks.

`pg_stat_statements` is the precedent for the other half: its `_PG_init` returns
early (`if (!process_shared_preload_libraries_in_progress) return;`) so that the
shmem-dependent machinery is registered only at postmaster time, while the
extension's SQL functions stay creatable/callable regardless and "must protect
themselves against being called" when the library isn't active. That is the model
for `deltax_run_maintenance()` being callable via fmgr in session mode (see
"Background worker") and for the self-guarding read path (#3). pg_deltax differs
in that its *query hooks* must be installed in both modes (auto_explain pattern),
and only the worker + shmem registration is gated behind the flag
(pg_stat_statements pattern).

Listing pg_deltax in **both** preload lists is harmless — Postgres won't re-run
`_PG_init` for an already-loaded library in a backend.

### 2. Query correctness is identical in both modes

All correctness-critical hooks are per-backend function pointers set in
`_PG_init`, so they behave the same whether the library was inherited from the
postmaster (`shared_preload`) or `dlopen`-ed at connection start
(`session_preload`). Parallel workers are covered too: Postgres replays the
leader's loaded-library set into each parallel worker, so the parallel
custom-scan path keeps working.

Cost difference: `session_preload` pays a per-connection `dlopen` + `_PG_init`
(the `.so` is in the OS page cache, so it's cheap; negligible behind a
connection pooler). `shared_preload` pays nothing per connection.

### 3. Mandatory runtime guard against silent data omission

Because `session_preload` can be mis-scoped, reading a pg_deltax-managed table in
a backend that does **not** have our hooks installed must **raise an error**
rather than return rows:

```
ERROR:  pg_deltax is not loaded in this session, cannot read compressed table "<t>"
HINT:   add 'pg_deltax' to session_preload_libraries (e.g.
        ALTER DATABASE <db> SET session_preload_libraries = 'pg_deltax')
        or to shared_preload_libraries.
```

This converts the dangerous silent-omission failure into a loud, actionable
error. (TimescaleDB takes the same stance — it refuses to operate un-preloaded
rather than misbehave.) The **guarantee** is firm: never serve partial data
silently. The **mechanism is the hard part**, for a fundamental reason:

> When a backend has *zero* pg_deltax code loaded, none of our code runs. We
> literally cannot raise our own error from a function pointer we never
> installed. So a process-local sentinel checked inside any of our hooks is
> useless for the case that matters most — the hooks are exactly what's absent.

Candidate mechanisms, and why most fail:

- **Event trigger** — does *not* fire on `SELECT` (DDL-only). Eliminated.
- **Process-local sentinel checked in a hook** — the hook doesn't run when
  un-loaded. Eliminated.
- **RLS `USING` qual / security-barrier view / `ON SELECT` rule** — these are the
  *only* core mechanisms that execute on a plain `SELECT` without our hooks.
  An RLS policy `USING (deltax.assert_loaded())` on each deltatable is the least
  invasive. But it has a subtle trap: calling that function via fmgr *dlopens the
  `.so` and runs `_PG_init`*, which would set a naive "hooks installed" sentinel
  **before** the function body runs — so the check always passes and never fires.
  To work, the qual must test a **per-query flag set by the `set_rel_pathlist`
  hook** (i.e. "did our pathlist hook run for *this* plan?"), not a process-local
  sentinel. If the `.so` was only lazily loaded to call the qual function, the
  pathlist hook did not run for this query → flag unset → raise. The *next* query
  in the same session has hooks installed and works. This is workable but
  intricate (an RLS policy per deltatable, plus a per-query flag), so the
  mechanism is the **leading candidate** rather than a settled choice — it needs
  prototyping.

  **RLS bypass hole — prototype against this first.** RLS has two built-in
  bypasses that hit exactly the connections this guard most needs to protect:
  - **Superusers always bypass RLS** (it can't be forced on for them).
  - **The table owner bypasses RLS** unless the table has `FORCE ROW LEVEL
    SECURITY` set.

  `pg_dump`, ETL jobs, logical-replication initial sync, and admin reads are
  frequently run as the table owner or a superuser — precisely the backup/
  replication scenarios the "silent zero rows" section below worries about. For
  those roles an RLS `USING (deltax.assert_loaded())` qual would never fire, so
  they'd still silently get zero rows from compressed partitions. Enabling
  `FORCE ROW LEVEL SECURITY` closes the *owner* hole but not the *superuser* one.
  So before treating RLS as settled, prototype it specifically against a
  superuser dump connection with no hooks loaded, and decide whether the residual
  superuser gap is acceptable or forces a different mechanism.

Backup / replication interaction:

- This silent-empty behavior already affects `pg_dump` and logical-replication
  initial sync in *any* backend without hooks — including in **full mode** today,
  if a tool connects in a way that bypasses the hooks. A dump connection without
  the custom scan dumps **zero rows** for every compressed partition.
- The guard turning that into a loud error is strictly safer, but it is an
  **operational behavior change**: backup/ETL/replication tooling must also load
  pg_deltax (e.g. via the same `session_preload_libraries` scope), or those jobs
  start erroring instead of silently producing empty output. Document this
  explicitly; it is arguably a bug-fix for full mode too.

### 4. Configuration, practically

Full mode (default, matches today; full feature set incl. worker + shared cache):

```
shared_preload_libraries = 'pg_deltax'      # postgresql.conf; needs restart
```

Session mode (no restart; per-database; query correctness, no static worker, no
shared cache):

```sql
-- 1. Catalog + SQL functions. session_preload loads the .so (hooks), but the
--    deltax catalog and the deltax_* functions only exist after CREATE EXTENSION.
CREATE EXTENSION pg_deltax;

-- 2. Load the library on new connections to "analytics"; no restart.
ALTER DATABASE analytics SET session_preload_libraries = 'pg_deltax';
```

Inert GUCs in session mode: `pg_deltax.target_database`, `pg_deltax.blob_cache_mb`,
and `pg_deltax.blob_cache_shards` are all `PGC_POSTMASTER` context. In session mode
they cannot be set per-database/session (PG rejects or ignores the change) and
have no effect — there is no postmaster launcher to read `target_database` and
no shared cache. Treat them as full-mode-only knobs.

Cluster-wide session mode (closest to shared_preload minus the postmaster powers):

```sql
ALTER SYSTEM SET session_preload_libraries = 'pg_deltax';
SELECT pg_reload_conf();
```

### What each mode gives you

| | full mode (`shared_preload`) | session mode (`session_preload`) |
| --- | --- | --- |
| Query correctness (custom scan, agg, utility hooks) | ✅ | ✅ |
| Background maintenance (drain/premake/compress/retention) | ✅ static worker | scheduled externally (pg_cron / cron → `deltax_run_maintenance()`) |
| Shared blob cache | ✅ | ❌ off for now (perf only) |
| Server restart to enable | yes | no |
| Scope | whole cluster (all DBs) | chosen DB/role only |

### Blob cache: out of scope for now

In session mode the shared blob cache is simply **off**. It's a performance
feature and the code already has a "cache unavailable" path
(`blob_cache` `CACHE_USABLE` / `configured_bytes() == 0`), so correctness is
unaffected — only cold-read latency. Future options (not now): a runtime DSM
segment (`GetNamedDSMSegment`, PG17+) instead of a fixed reservation, or a
per-backend local cache.

## Background worker (session mode): external scheduling

In session mode the static launcher (`worker::register_bgworker` →
`deltax_launcher_main` → `deltax_worker_main`) cannot be registered, so there is
no automatic maintenance process.

**Decision (for now): session mode does not run a worker — the user schedules
maintenance externally, e.g. with pg_cron.** It's the simplest option, owns no
long-lived process of ours, survives restarts (the scheduler persists its own
schedule), and is trivially observable (it's just a query that runs on a
schedule). Full mode is unchanged — its static worker keeps running
automatically and needs none of this.

### Prerequisite: a SQL-callable maintenance entry point

The worker's per-deltatable job (the loop in `deltax_worker_main`) is:

1. `drain_default_partition` — move default-partition rows into real partitions
2. `partition::ensure_future_partitions` — pre-create future partitions (premake)
3. `compress::auto_compress_partitions` (+ `stats::write_table_stats`)
4. `partition::auto_drop_partitions` — retention

Today only `drain_default_partition` and `deltax_compress_all_partitions` are
individually SQL-callable; premake and retention are reachable **only** from
inside the worker loop. So session mode needs that loop factored into one
SQL-callable function — `deltax_run_maintenance()` — that runs all four steps
for every deltatable in the **current database**. Full mode's worker should call
the *same* function, so there is a single maintenance code path (this factoring
is independently useful for tests and manual ops).

Three behaviors of the worker loop (`deltax_worker_main`) must carry over to the
factored function:

1. **Transaction model.** The worker wraps *all* deltatables × all four steps in
   a **single** `BackgroundWorker::transaction(...)` per 60s tick. A regular
   `#[pg_extern]` function (one caller transaction) reproduces this; a
   `PROCEDURE` with internal `COMMIT`s is not required, and the pg_cron
   `SELECT deltax_run_maintenance()` form works directly. One invocation holds
   locks and runs compression for every table in one transaction — acceptable at
   current scale; revisit only if per-table commit isolation becomes desirable.
2. **Per-table error isolation — partial today; must be *added*, not merely
   preserved.** The current worker loop is only half-isolated, and the gap
   matters more under an external scheduler than it does for the worker:
   - `drain_default_partition` and `partition::ensure_future_partitions` return
     `Result`; the loop matches the `Err`, logs it, and continues. These two
     steps are isolated.
   - `compress::auto_compress_partitions` and `partition::auto_drop_partitions`
     return a plain count and raise Postgres `ERROR`s internally (`.expect()` /
     `pgrx::error!()` in `compress_partition_impl` and the retention path). These
     are **not** caught — there is no `PgTryBuilder`/subtransaction wrapping any
     table or step in the loop.

   So a compression or retention `ERROR` on one deltatable longjmps out of the
   single per-tick `BackgroundWorker::transaction` and aborts maintenance for
   **every** table that tick — not just the broken one. In the worker this is
   masked by `set_restart_time(60s)`: the process restarts and retries next tick.
   A bare `#[pg_extern] deltax_run_maintenance()` called from pg_cron has no such
   safety net — one bad table fails the whole call on every run, with no
   per-table retry.

   Therefore the factored function should **add** real per-table isolation that
   the worker lacks: wrap each table's work (or each step) in a subtransaction
   (`PgTryBuilder` / `BeginInternalSubTransaction`) so a single failure rolls back
   only that unit and the loop logs-and-continues. This is new code, not a
   straight lift of the existing loop. If we instead lift the loop verbatim, the
   worker should ideally gain the same isolation so both paths behave identically.

   **Implemented** (`worker::run_maintenance_pass` / `maintain_one_table` /
   `run_in_subtransaction`): the loop body is factored into a shared function
   that both the worker and `deltax_run_maintenance()` call. Each deltatable is
   wrapped in an internal subtransaction modeled on PL/pgSQL's `BEGIN …
   EXCEPTION` block, so a Postgres error in any step rolls back only that table's
   work and the pass logs-and-continues. The worker now gets this isolation too.
3. **Replica guard.** The worker skips the whole pass when `pg_is_in_recovery()`.
   An external scheduler firing `deltax_run_maintenance()` against a standby would
   attempt DDL and error, so this guard belongs **inside the function** (no-op on
   a replica). **Implemented** inside `run_maintenance_pass`.
4. **Pass-level mutual exclusion (added during implementation).** In full mode
   the static worker is always running, so a manual or scheduled
   `deltax_run_maintenance()` call would run the same detach/attach/compress DDL
   concurrently with a worker tick and could **deadlock** (observed: worker
   holding a freshly-created partition's lock while waiting on the caller's
   catalog-row lock, and vice-versa). The factored pass therefore takes a
   transaction-level advisory lock (`pg_try_advisory_xact_lock`, fixed
   pg_deltax-namespaced key) before touching any table; whoever loses skips the
   pass. Because the advisory lock is always acquired before any table-level
   lock, two maintenance passes can never deadlock on the DDL, and a skipped
   redundant pass is harmless (maintenance is periodic and idempotent). This
   makes `deltax_run_maintenance()` safe to call manually even in full mode.

Note on loading: a scheduler's backend (e.g. the pg_cron worker) is not a
`session_preload` client connection, but calling `deltax.deltax_run_maintenance()`
loads pg_deltax on demand via fmgr (running `_PG_init`), so the function works
regardless of preload mode. The maintenance steps operate on uncompressed data
and the catalog, so they don't need the custom-scan read hook anyway.

### Scheduling with pg_cron

pg_cron is itself a background-worker scheduler, so it needs
`shared_preload_libraries = 'pg_cron'` (a one-time restart) and
`CREATE EXTENSION pg_cron;` in its scheduler database (default `postgres`). It's
pre-installed / allow-listed on most managed platforms.

```sql
-- Run pg_deltax maintenance once a minute in the `analytics` database. This
-- matches the built-in worker's 60s cadence; deltax_run_maintenance() processes
-- every deltatable in the database it runs in, so it's one job per database.
SELECT cron.schedule_in_database(
    job_name => 'pg_deltax-maintenance-analytics',
    schedule => '* * * * *',                          -- standard cron: every minute
    command  => 'SELECT deltax.deltax_run_maintenance()',
    database => 'analytics'
);

-- One job per database that uses pg_deltax:
SELECT cron.schedule_in_database('pg_deltax-maintenance-metrics', '* * * * *',
    'SELECT deltax.deltax_run_maintenance()', 'metrics');

-- Inspect / remove:
SELECT jobid, jobname, schedule, database FROM cron.job;
SELECT cron.unschedule('pg_deltax-maintenance-analytics');
```

Notes:
- **Privileges:** the built-in worker runs as superuser; a pg_cron job runs as
  the role that owns it (or the `username` argument). That role must be able to
  manage the deltatables — create/drop partitions and write the `deltax` catalog
  — so schedule the job as a superuser or a role with equivalent rights over
  those tables.
- **Cadence:** standard cron granularity is one minute. Recent pg_cron also
  accepts interval syntax (e.g. `'30 seconds'`) if you want to track the 60s
  loop more tightly; once a minute is normally fine.
- **No pg_cron?** Any external scheduler works — a cron job or job runner that
  runs `psql -c 'SELECT deltax.deltax_run_maintenance()'` against each database
  on an interval. The only requirement is "call the function periodically."

### Deferred alternatives

Considered and deferred; revisit if "no automatic maintenance without setup"
proves too sharp an edge for session-mode users:

- **Dynamic worker on demand** — `RegisterDynamicBackgroundWorker` launched from
  the session-mode `_PG_init` / `deltax_create_table`, kept singleton via an
  advisory lock and re-launched on connect after a restart. Hands-off, but we'd
  own the singleton + restart + per-DB-launch logic.
- **Hybrid** — ship both: pg_cron as the documented default plus an opt-in
  `deltax_start_worker()` doing the dynamic launch for users who want zero
  scheduling setup.

If we ever pick these up, the open items are: the singleton mechanism (advisory
lock vs `pg_stat_activity` scan vs heartbeat row), multi-DB targeting (full mode
uses `pg_deltax.target_database`; session mode has no postmaster launcher to read
it), and observability (a `deltax_status()` view showing the active mode and the
last maintenance run).

## Out of scope for this document

- Shared blob cache in session mode (left off; see above).
- The runtime guard (#3): the *guarantee* (never serve partial data silently) is
  decided, but the *mechanism* is a genuine open problem — see the analysis in #3.
  The leading candidate is an RLS `USING` qual that checks a per-query
  "pathlist-hook-ran" flag; needs prototyping before it's considered settled.
- Backup/replication interaction with the guard (#3) — needs an operational note
  in user docs once the mechanism lands.
