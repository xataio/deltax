# Preload Modes: `shared_preload_libraries` vs `session_preload_libraries`

Status: design / proposal, **Phase 1 implemented**. The "Decided" section is
settled, including the runtime-guard question (#3): we decided **not** to build a
guard and to document the limitation instead. The one item still genuinely open
is session-mode background-worker scheduling (options listed below).

Phase 1 (shipped): the dual-mode `_PG_init` branch (#1), the factored
`deltax_run_maintenance()` SQL entry point with per-table subtransaction
isolation + replica guard + pass-level advisory-lock mutual exclusion, and the
worker reusing the same code path.

Known limitation (decided, #3): in session mode a mis-scoped or hook-less backend
(notably superuser/owner `pg_dump`, ETL, and replication connections) can
**silently return zero rows from compressed partitions**. There is no runtime
guard against this — it is documented and mitigated by configuration, not code.
Users who can't guarantee every reader loads the library should use full mode.

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
differently). We considered a runtime guard that fails loudly instead of
returning partial data, but **decided against building one** — it is both hard to
implement and ineffective for the highest-stakes readers (superuser/owner dumps).
This is therefore a **documented limitation** of session mode, mitigated by
configuration; see §3 for the full rationale and the operational guidance.

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
that *was* compressed, later read by a backend with no hooks. This bounds the
exposure of the documented limitation in §3.

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

    if process_shared_preload_libraries_in_progress:
        blob_cache::register_hooks()     # RequestAddinShmemSpace; postmaster-only
        worker::register_bgworker()      # static launcher; postmaster-only
    # else (session_preload / LOAD / fmgr): query hooks only —
    #   no static worker (maintenance via deltax_run_maintenance(); see below),
    #   blob cache stays off (see "Blob cache").
```

This matches the shipped `_PG_init`: the query hooks (`scan::register_hook`,
`scan::register_executor_start_hook`, `copy::register_process_utility_hook`) are
installed unconditionally, and only `blob_cache::register_hooks()` +
`worker::register_bgworker()` are gated behind the flag. (There is no
"hooks-installed" sentinel — that idea belonged to the runtime guard in #3, which
was dropped.)

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
"Background worker"). pg_deltax differs in that its *query hooks* must be
installed in both modes (auto_explain pattern), and only the worker + shmem
registration is gated behind the flag (pg_stat_statements pattern).

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

### 3. Known limitation: no runtime guard against silent data omission (decided)

**Decision: we do not implement a runtime guard. Session mode ships with this as
a documented limitation.** A backend that reads a pg_deltax-managed table without
our hooks installed will **silently return zero rows for every compressed
partition** (recent uncompressed partitions read normally). We accept that and
document it rather than trying to detect it. The rest of this section records
*why* a guard was considered and then rejected, so the decision isn't revisited
blindly.

What an ideal guard would do is convert the silent omission into a loud error:

```
ERROR:  pg_deltax is not loaded in this session, cannot read compressed table "<t>"
```

The trouble is that the guard is both hard to build *and*, in the form that's
buildable, ineffective for the cases that matter most.

**Why it's hard to build.** When a backend has *zero* pg_deltax code loaded, none
of our code runs — we literally cannot raise our own error from a function
pointer we never installed. A process-local sentinel checked inside a hook is
useless for exactly the case that matters: the hooks are what's absent. That
rules out every extension-side mechanism. The only things PostgreSQL core
evaluates on a plain `SELECT` without our code are catalog-resident: an **RLS
`USING` qual**, a **security-barrier view**, or an **`ON SELECT` rule**. An RLS
policy `USING (deltax.assert_loaded())` per deltatable is the least invasive of
those — but it carries a subtle trap (calling the qual function fmgr-loads the
`.so` and runs `_PG_init`, so a naive "are we loaded?" check always passes by the
time it runs). To work at all it would need a **per-query flag set by the
`set_rel_pathlist` hook** ("did our pathlist hook run for *this* plan?"), plus a
policy on every deltatable.

**Why even the buildable form doesn't really help.** RLS is bypassed for
**superusers** (always — it can't be forced on) and for the **table owner**
(unless `FORCE ROW LEVEL SECURITY` is set). `pg_dump`, ETL, logical-replication
initial sync, and admin reads are frequently run as a superuser or the table
owner — precisely the connections most likely to arrive without hooks. For those,
the `USING` qual never even evaluates, so they'd still get silent zero rows.
`FORCE ROW LEVEL SECURITY` closes the owner gap but **not** the superuser one, and
superuser dumps are common. So the one mechanism that's technically feasible
fails open for the highest-stakes consumer. A rule/`ON SELECT` view *would* fire
for superusers too, but at the cost of replacing every deltatable with a view —
too invasive to justify for this.

Given hard-to-build + ineffective-where-it-counts, the guard isn't worth its
complexity. We document the limitation instead (see below) and rely on correct
configuration.

**This is not new to session mode.** The same silent-empty behavior already
affects `pg_dump` and logical-replication initial sync in **full mode** today if
a tool connects in a way that bypasses the hooks — a dump from a hook-less
backend writes zero rows for every compressed partition. Session mode widens the
exposure (mis-scoping is easier) but does not introduce a new class of bug.

**Operational guidance to document for users.** The mitigation is configuration,
not code:

- Scope the library at the **database** level — `ALTER DATABASE <db> SET
  session_preload_libraries = 'pg_deltax'` — not per-role, so every backend on
  the database loads it regardless of which role connects (this is the main
  mis-scoping vector; see Motivation).
- Ensure **backup / ETL / replication / admin** tooling connects to a database
  (or cluster) where pg_deltax is loaded — i.e. they must be inside the same
  `session_preload_libraries` (or `shared_preload_libraries`) scope. A dump taken
  from a hook-less backend is **silently incomplete** for compressed partitions.
- If you cannot guarantee that for all readers, prefer **full mode**
  (`shared_preload_libraries`), where the postmaster loads the library into every
  backend and the failure mode cannot occur.

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

Absent GUCs in session mode: `pg_deltax.target_database`, `pg_deltax.blob_cache_mb`,
and `pg_deltax.blob_cache_shards` are all `PGC_POSTMASTER` context and are
**not defined at all** in session mode — `SHOW` on them errors with
"unrecognized configuration parameter". This is not just a nicety: PostgreSQL
*FATALs* ("cannot create PGC_POSTMASTER variables after startup") if a
`PGC_POSTMASTER` GUC is defined outside postmaster startup, so `_PG_init` must
gate the `define_*_guc` calls for these three behind
`process_shared_preload_libraries_in_progress` — defining them in a
session_preload / LOAD / fmgr backend would crash that backend. (The
`PGC_USERSET` / `PGC_SUSET` GUCs — `mock_now`, `parallel_workers`, etc. — are
defined unconditionally and work in both modes.) They are full-mode-only knobs:
there is no postmaster launcher to read `target_database` and no shared cache.

Cluster-wide session mode (closest to shared_preload minus the postmaster powers):

```sql
ALTER SYSTEM SET session_preload_libraries = 'pg_deltax';
SELECT pg_reload_conf();
```

### What each mode gives you

| | full mode (`shared_preload`) | session mode (`session_preload`) |
| --- | --- | --- |
| Query correctness (custom scan, agg, utility hooks) | ✅ | ✅ when loaded |
| Safe if a reader has no hooks loaded | ✅ postmaster loads every backend | ⚠️ silent zero rows from compressed partitions — no guard (§3); mitigate by scoping at DB level |
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

Behaviors the factored function must get right. The first three carry over from
the worker loop (`deltax_worker_main`); the last two were added during
implementation to make the SQL-callable path safe to invoke directly:

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
   transaction-level advisory lock (`pg_try_advisory_xact_lock`) before touching
   any table; whoever loses skips the pass. Because the advisory lock is always
   acquired before any table-level lock, two maintenance passes can never
   deadlock on the DDL, and a skipped redundant pass is harmless (maintenance is
   periodic and idempotent). This makes `deltax_run_maintenance()` safe to call
   manually even in full mode.

   The key is **per-database** (`maintenance_lock_key()` = a fixed pg_deltax tag
   in the high 32 bits, `MyDatabaseId` in the low 32). Advisory locks are
   cluster-wide, so a single fixed key would serialize maintenance across *every*
   database — the multiple workers in a `target_database` list, or one pg_cron
   job per database — even though they touch disjoint tables. Folding in the
   database OID confines mutual exclusion to within a single database.
5. **search_path safety.** The maintenance SQL uses unqualified names (`now()`,
   `pg_tables`, operators, casts). The worker runs as superuser and pins
   `search_path = pg_catalog, pg_temp` at session start to stop a planted object
   from shadowing them; the SQL-callable path runs with the *caller's*
   search_path (a pg_cron job typically runs as superuser too), so
   `run_maintenance_pass` issues `SET LOCAL search_path = pg_catalog, pg_temp` at
   the top of every pass. `SET LOCAL` reverts at transaction end, so it never
   leaks into the caller's session.

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
- The runtime guard (#3) — **resolved as a non-goal**: no guard will be built;
  the silent-zero-rows behavior is a documented limitation of session mode (see
  §3 for the rationale and the user-facing operational guidance). If a future
  need forces a reconsideration, the only feasible mechanism was an RLS `USING`
  qual with a per-query "pathlist-hook-ran" flag, and its fatal flaw was the RLS
  superuser/owner bypass — start there.
