//! ProcessUtility-hook helpers for `ALTER TABLE` / `RENAME` / `SET SCHEMA`
//! on pg_deltax-managed tables. See `dev/docs/SCHEMA_CHANGES.md`.
//!
//! Classifies each ALTER subcommand against the tier matrix:
//!
//! * Tier 1 — operations safe to pass through to PG with no blob touch.
//!   Some need post-success catalog bookkeeping (e.g. RENAME COLUMN
//!   updates `deltax.deltax_deltatable.segment_by` if the renamed
//!   column is referenced there).
//! * Tier 2 — `DROP COLUMN` for a non-key column. Passes through to PG
//!   and tombstones the matching descriptor entry on every child
//!   partition.
//! * Tier 3 — operations that would corrupt compressed blobs or
//!   silently misvalidate against empty heaps. We `ereport(ERROR)` with
//!   `ERRCODE_FEATURE_NOT_SUPPORTED` and a HINT pointing at the
//!   decompress→ALTER→recompress recipe in `SCHEMA_CHANGES.md`.
//!
//! The actual ProcessUtility hook lives in `copy.rs::deltax_process_utility`;
//! this module exports `handle_alter_table` / `handle_rename` /
//! `handle_alter_object_schema` for it to dispatch into.

use pgrx::PgLogLevel;
use pgrx::PgSqlErrorCode;
use pgrx::pg_sys;
use pgrx::pg_sys::AlterTableType;
use pgrx::pg_sys::ConstrType;
use pgrx::pg_sys::ObjectType;
use pgrx::pg_sys::panic::ErrorReport;
use std::cell::Cell;
use std::ffi::CStr;

use crate::catalog::{self, DeltatableInfo};

thread_local! {
    /// When true, the ProcessUtility-hook classifier skips Tier 3 checks
    /// and treats every ALTER as `NotOurTable`. Set by pg_deltax-internal
    /// DDL (worker partition rotation, partition.rs swap+detach paths)
    /// so our own operations don't trip our policy.
    static BYPASS_DDL_HOOK: Cell<bool> = const { Cell::new(false) };
}

/// Run a closure with the DDL-hook bypass active. Restores the previous
/// value on exit (nested calls compose correctly). Use this around any
/// pg_deltax-internal SPI call that issues `ALTER TABLE` / `RENAME` /
/// `DETACH PARTITION` / `ATTACH PARTITION` on a registered deltatable.
pub(crate) fn with_bypass<F, R>(f: F) -> R
where
    F: FnOnce() -> R,
{
    BYPASS_DDL_HOOK.with(|c| {
        let prev = c.get();
        c.set(true);
        let result = f();
        c.set(prev);
        result
    })
}

fn bypass_active() -> bool {
    if BYPASS_DDL_HOOK.with(|c| c.get()) {
        return true;
    }
    // PG sets `creating_extension = true` while CREATE EXTENSION /
    // ALTER EXTENSION UPDATE is running. Our own extension_sql! block
    // issues `ALTER TABLE deltax.deltax_partition ADD COLUMN ...`
    // migrations that would otherwise trip our hook (and fail trying
    // to query columns that don't exist yet because the migration
    // adding them hasn't run).
    unsafe { pg_sys::creating_extension }
}

/// What the ProcessUtility hook should do after our classifier finishes.
pub(crate) enum AlterDisposition {
    /// Statement does not target a registered deltatable — just chain.
    NotOurTable,
    /// Pass straight through to `standard_ProcessUtility`. After success,
    /// run each `PostAction` to mirror the change into our catalog.
    Tier1 { post_actions: Vec<PostAction> },
    /// Block before PG executes anything. `op_name` and `table` describe
    /// what was rejected; raise via `raise_tier3`.
    Tier3 {
        op_name: &'static str,
        table: String,
    },
}

/// Catalog-mirroring action run after PG's `standard_ProcessUtility`
/// applied a Tier 1 change. Closures are not used so the disposition is
/// `Send`-friendly and inspectable in tests.
pub(crate) enum PostAction {
    RenameColumn {
        ht_id: i32,
        old: String,
        new: String,
    },
    RenameTable {
        ht_id: i32,
        new: String,
    },
    SetSchema {
        ht_id: i32,
        new: String,
    },
    /// Tier 2 `DROP COLUMN` for a non-key column: flip `dropped: true`
    /// on the descriptor entry of every child partition whose
    /// `compressed_columns` JSONB names the dropped column. Orphan
    /// `_blobs`/`_colstats`/etc. rows for the descriptor's
    /// `compressed_col_idx` are left until the partition is recompressed
    /// (documented future-work GC item).
    TombstoneColumn {
        ht_id: i32,
        column_name: String,
    },
    /// `OWNER TO new_owner` on a deltatable: PG only re-owners the
    /// parent; cascade to every companion table in `_deltax_compressed.*`
    /// so admins don't have to chase the per-partition tables manually.
    /// `new_owner_sql` is the role spec as a quoted SQL fragment (e.g.
    /// `"alice"` or `CURRENT_USER`).
    CascadeOwnerToCompanions {
        ht_id: i32,
        new_owner_sql: String,
    },
    /// `GRANT … ON TABLE deltatable TO …` / `REVOKE …`: PG applies the
    /// privilege change to the parent only; cascade onto every companion
    /// table so a role granted SELECT on `metrics` can also read the
    /// compressed companions. `prefix` and `suffix` are pre-built SQL
    /// fragments — the table name is substituted at apply time.
    CascadeGrantToCompanions {
        ht_id: i32,
        grant_prefix: String,
        grant_suffix: String,
    },
}

/// Run the post-success bookkeeping for a Tier 1 disposition. Called by
/// the ProcessUtility hook after `standard_ProcessUtility` returns
/// without error.
pub(crate) fn apply_post_actions(actions: Vec<PostAction>) {
    if actions.is_empty() {
        return;
    }
    pgrx::Spi::connect_mut(|client| {
        for act in actions {
            match act {
                PostAction::RenameColumn { ht_id, old, new } => {
                    if let Err(e) = catalog::rename_column_in_deltatable(client, ht_id, &old, &new)
                    {
                        pgrx::warning!(
                            "pg_deltax: failed to mirror RENAME COLUMN into catalog \
                             (ht={}, {} -> {}): {:?}",
                            ht_id,
                            old,
                            new,
                            e,
                        );
                    }
                }
                PostAction::RenameTable { ht_id, new } => {
                    if let Err(e) = catalog::rename_deltatable(client, ht_id, &new) {
                        pgrx::warning!(
                            "pg_deltax: failed to mirror RENAME TABLE into catalog \
                             (ht={}, -> {}): {:?}",
                            ht_id,
                            new,
                            e,
                        );
                    }
                }
                PostAction::SetSchema { ht_id, new } => {
                    if let Err(e) = catalog::set_deltatable_schema(client, ht_id, &new) {
                        pgrx::warning!(
                            "pg_deltax: failed to mirror SET SCHEMA into catalog \
                             (ht={}, -> {}): {:?}",
                            ht_id,
                            new,
                            e,
                        );
                    }
                }
                PostAction::TombstoneColumn { ht_id, column_name } => {
                    if let Err(e) =
                        catalog::tombstone_column_in_descriptor(client, ht_id, &column_name)
                    {
                        pgrx::warning!(
                            "pg_deltax: failed to tombstone DROP COLUMN in descriptor \
                             (ht={}, col={}): {:?}",
                            ht_id,
                            column_name,
                            e,
                        );
                    }
                }
                PostAction::CascadeOwnerToCompanions {
                    ht_id,
                    new_owner_sql,
                } => {
                    if let Err(e) = cascade_owner(client, ht_id, &new_owner_sql) {
                        pgrx::warning!(
                            "pg_deltax: failed to cascade OWNER TO {} onto companions \
                             (ht={}): {:?}",
                            new_owner_sql,
                            ht_id,
                            e,
                        );
                    }
                }
                PostAction::CascadeGrantToCompanions {
                    ht_id,
                    grant_prefix,
                    grant_suffix,
                } => {
                    if let Err(e) = cascade_grant(client, ht_id, &grant_prefix, &grant_suffix) {
                        pgrx::warning!(
                            "pg_deltax: failed to cascade GRANT/REVOKE onto companions \
                             (ht={}, sql={}<companion>{}): {:?}",
                            ht_id,
                            grant_prefix,
                            grant_suffix,
                            e,
                        );
                    }
                }
            }
        }
    });
}

/// Issue `ALTER TABLE companion OWNER TO new_owner_sql` against every
/// companion table for the given deltatable. The
/// `crate::ddl::with_bypass` wrap stops our own ALTER from tripping
/// the policy hook recursively.
fn cascade_owner(
    client: &mut pgrx::spi::SpiClient,
    ht_id: i32,
    new_owner_sql: &str,
) -> pgrx::spi::SpiResult<()> {
    let companions = catalog::compressed_companion_tables(client, ht_id)?;
    for fqn in companions {
        with_bypass(|| {
            client.update(
                &format!("ALTER TABLE {} OWNER TO {}", fqn, new_owner_sql),
                None,
                &[],
            )
        })?;
    }
    Ok(())
}

/// Re-issue a `GRANT` or `REVOKE` SQL fragment against every companion
/// table for the given deltatable. `prefix` ends with `ON TABLE ` and
/// `suffix` starts with ` TO ` / ` FROM ` — the companion FQN is
/// concatenated between them per call.
fn cascade_grant(
    client: &mut pgrx::spi::SpiClient,
    ht_id: i32,
    prefix: &str,
    suffix: &str,
) -> pgrx::spi::SpiResult<()> {
    let companions = catalog::compressed_companion_tables(client, ht_id)?;
    for fqn in companions {
        client.update(&format!("{}{}{}", prefix, fqn, suffix), None, &[])?;
    }
    Ok(())
}

/// Format and raise the Tier 3 error. Never returns.
pub(crate) fn raise_tier3(op: &str, table: &str) -> ! {
    ErrorReport::new(
        PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED,
        format!(
            "pg_deltax: {} on deltatable {:?} is not supported \
             while any partition is compressed",
            op, table
        ),
        pgrx::function_name!(),
    )
    .set_hint(
        "Decompress every compressed partition with \
         deltax.deltax_decompress_partition(name), run the ALTER, \
         then recompress with deltax.deltax_compress_partition(name). \
         See dev/docs/SCHEMA_CHANGES.md for the full recipe.",
    )
    .report(PgLogLevel::ERROR);
    unreachable!();
}

/// Resolve a RangeVar to (schema_name, table_name) by OID lookup. Returns
/// `None` for an invalid/missing relation — the caller treats that as
/// `NotOurTable` and chains straight through.
unsafe fn resolve_rangevar(rv: *mut pg_sys::RangeVar) -> Option<(String, String)> {
    unsafe {
        if rv.is_null() {
            return None;
        }
        let oid = pg_sys::RangeVarGetRelidExtended(
            rv,
            pg_sys::NoLock as i32,
            pg_sys::RVROption::RVR_MISSING_OK,
            None,
            std::ptr::null_mut(),
        );
        if oid == pg_sys::InvalidOid {
            return None;
        }
        let ns_oid = pg_sys::get_rel_namespace(oid);
        let ns_ptr = pg_sys::get_namespace_name(ns_oid);
        let rel_ptr = pg_sys::get_rel_name(oid);
        if ns_ptr.is_null() || rel_ptr.is_null() {
            return None;
        }
        let ns = CStr::from_ptr(ns_ptr).to_string_lossy().into_owned();
        let rel = CStr::from_ptr(rel_ptr).to_string_lossy().into_owned();
        Some((ns, rel))
    }
}

/// Read a NUL-terminated C string into a Rust String. Returns empty on NULL.
unsafe fn cstr_to_string(ptr: *const std::os::raw::c_char) -> String {
    unsafe {
        if ptr.is_null() {
            String::new()
        } else {
            CStr::from_ptr(ptr).to_string_lossy().into_owned()
        }
    }
}

/// The deltatable that owns the parsed RangeVar, plus a flag for "does
/// any partition currently hold compressed data." Tier 3 only applies
/// when the latter is true (after a full decompress, the table is
/// effectively a plain partitioned table and PG ops are safe).
struct AlterTarget {
    ht: DeltatableInfo,
    has_compressed_partitions: bool,
}

/// Look up the deltatable that owns the parsed RangeVar — the parent
/// itself or a registered child partition. Returns `None` for anything
/// pg_deltax doesn't manage.
unsafe fn lookup_target(rv: *mut pg_sys::RangeVar) -> Option<AlterTarget> {
    let (schema, table) = unsafe { resolve_rangevar(rv) }?;
    pgrx::Spi::connect(|client| {
        let ht = match catalog::get_deltatable(client, &schema, &table)? {
            Some(ht) => ht,
            None => match catalog::get_partition_by_name(client, &schema, &table)? {
                Some(part) => match catalog::get_deltatable_by_id(client, part.deltatable_id)? {
                    Some(ht) => ht,
                    None => return Ok::<Option<AlterTarget>, pgrx::spi::Error>(None),
                },
                None => return Ok(None),
            },
        };

        let has_compressed = client
            .select(
                "SELECT EXISTS(
                    SELECT 1 FROM deltax.deltax_partition
                    WHERE deltatable_id = $1 AND is_compressed
                )",
                None,
                &[ht.id.into()],
            )?
            .first()
            .get_one::<bool>()?
            .unwrap_or(false);
        Ok(Some(AlterTarget {
            ht,
            has_compressed_partitions: has_compressed,
        }))
    })
    .ok()
    .flatten()
}

/// Classify an `ALTER TABLE` statement against the Tier 1 / Tier 3
/// matrix and return what the ProcessUtility hook should do.
pub(crate) unsafe fn handle_alter_table(stmt: *mut pg_sys::AlterTableStmt) -> AlterDisposition {
    unsafe {
        if stmt.is_null() || bypass_active() {
            return AlterDisposition::NotOurTable;
        }
        // Only intercept relkind=table. ALTER INDEX / ALTER VIEW share the
        // node type but routing them through here would surprise users.
        if (*stmt).objtype != ObjectType::OBJECT_TABLE {
            return AlterDisposition::NotOurTable;
        }
        let target = match lookup_target((*stmt).relation) {
            Some(t) => t,
            None => return AlterDisposition::NotOurTable,
        };

        let cmds = (*stmt).cmds;
        if cmds.is_null() {
            return AlterDisposition::Tier1 {
                post_actions: Vec::new(),
            };
        }

        let mut post_actions: Vec<PostAction> = Vec::new();
        let len = (*cmds).length;
        for i in 0..len {
            let cmd = pg_sys::list_nth(cmds, i) as *mut pg_sys::AlterTableCmd;
            if cmd.is_null() {
                continue;
            }
            match classify_at_subcommand(cmd, &target.ht) {
                SubDisposition::Tier1 { post_action } => {
                    if let Some(a) = post_action {
                        post_actions.push(a);
                    }
                }
                SubDisposition::Tier3 { op_name } => {
                    // Tier 3 ops only block while at least one partition
                    // is compressed — after the user runs the decompress
                    // recipe, the ALTER is safe and passes through.
                    if target.has_compressed_partitions {
                        return AlterDisposition::Tier3 {
                            op_name,
                            table: format!("{}.{}", target.ht.schema_name, target.ht.table_name),
                        };
                    }
                    // No compressed partitions — let PG handle it. No
                    // catalog post-action needed for these subcommands.
                }
            }
        }

        AlterDisposition::Tier1 { post_actions }
    }
}

/// Classify a `RENAME` statement (`OBJECT_COLUMN` or `OBJECT_TABLE`).
pub(crate) unsafe fn handle_rename(stmt: *mut pg_sys::RenameStmt) -> AlterDisposition {
    unsafe {
        if stmt.is_null() || bypass_active() {
            return AlterDisposition::NotOurTable;
        }
        let target = match lookup_target((*stmt).relation) {
            Some(t) => t,
            None => return AlterDisposition::NotOurTable,
        };

        let new = cstr_to_string((*stmt).newname);
        let table_fqn = format!("{}.{}", target.ht.schema_name, target.ht.table_name);

        match (*stmt).renameType {
            ObjectType::OBJECT_TABLE => AlterDisposition::Tier1 {
                post_actions: vec![PostAction::RenameTable {
                    ht_id: target.ht.id,
                    new,
                }],
            },
            ObjectType::OBJECT_COLUMN => {
                let old = cstr_to_string((*stmt).subname);
                // segment_by, order_by, and time_column are embedded by
                // name in the `_meta` companion table — renaming them
                // while any partition is compressed would require
                // rewriting that table's column shape.
                let touches_key = target.ht.time_column == old
                    || target.ht.segment_by.iter().any(|c| c == &old)
                    || target.ht.order_by.iter().any(|c| c == &old);
                if touches_key && target.has_compressed_partitions {
                    return AlterDisposition::Tier3 {
                        op_name: "RENAME COLUMN referenced by segment_by / order_by / time_column",
                        table: table_fqn,
                    };
                }
                AlterDisposition::Tier1 {
                    post_actions: vec![PostAction::RenameColumn {
                        ht_id: target.ht.id,
                        old,
                        new,
                    }],
                }
            }
            // RENAME CONSTRAINT, RENAME INDEX, etc. — pass through with
            // no catalog work (we don't track these in deltax catalog).
            _ => AlterDisposition::Tier1 {
                post_actions: Vec::new(),
            },
        }
    }
}

/// Classify a `GRANT` / `REVOKE` statement. We only look at table-level
/// (`OBJECT_TABLE`) grants whose target list contains at least one
/// registered deltatable. Each matching target produces a
/// `PostAction::CascadeGrantToCompanions` so PG's own GRANT runs first
/// (on the parent) and then we mirror onto every companion table.
///
/// Bypassed cases (pass through with no cascade):
/// - `GRANT ALL ON ALL TABLES IN SCHEMA …` (`ACL_TARGET_ALL_IN_SCHEMA`)
///   — the companion schema is separate from the user's; we don't second-
///   guess this.
/// - column-level grants (any `AccessPriv` carries a non-empty `cols`)
///   — they reference user-facing column names that aren't on companions.
/// - non-table object types (functions, schemas, etc.).
pub(crate) unsafe fn handle_grant(stmt: *mut pg_sys::GrantStmt) -> AlterDisposition {
    unsafe {
        if stmt.is_null() || bypass_active() {
            return AlterDisposition::NotOurTable;
        }
        if (*stmt).targtype != pg_sys::GrantTargetType::ACL_TARGET_OBJECT
            || (*stmt).objtype != ObjectType::OBJECT_TABLE
        {
            return AlterDisposition::NotOurTable;
        }

        // Detect column-level grants and bail. PG's grammar emits one
        // AccessPriv per priv keyword; cols is NIL for the table-level
        // form (`GRANT SELECT ...`), non-NIL for the column form
        // (`GRANT SELECT (col) ...`).
        let privs = (*stmt).privileges;
        if !privs.is_null() {
            let plen = (*privs).length;
            for i in 0..plen {
                let p = pg_sys::list_nth(privs, i) as *mut pg_sys::AccessPriv;
                if !p.is_null() && !(*p).cols.is_null() {
                    return AlterDisposition::NotOurTable;
                }
            }
        }

        let objects = (*stmt).objects;
        if objects.is_null() {
            return AlterDisposition::NotOurTable;
        }

        // Build the prefix/suffix once — they're the same for every
        // target deltatable in this statement.
        let prefix = build_grant_prefix(stmt);
        let suffix = build_grant_suffix(stmt);

        let mut post_actions: Vec<PostAction> = Vec::new();
        let olen = (*objects).length;
        for i in 0..olen {
            let rv = pg_sys::list_nth(objects, i) as *mut pg_sys::RangeVar;
            if let Some(target) = lookup_target(rv) {
                post_actions.push(PostAction::CascadeGrantToCompanions {
                    ht_id: target.ht.id,
                    grant_prefix: prefix.clone(),
                    grant_suffix: suffix.clone(),
                });
            }
        }

        if post_actions.is_empty() {
            return AlterDisposition::NotOurTable;
        }
        AlterDisposition::Tier1 { post_actions }
    }
}

/// Build the `GRANT <privs> ON TABLE ` / `REVOKE [GRANT OPTION FOR] <privs> ON TABLE `
/// prefix (everything up to and including the table-name slot).
unsafe fn build_grant_prefix(stmt: *mut pg_sys::GrantStmt) -> String {
    unsafe {
        let is_grant = (*stmt).is_grant;
        let revoke_grant_option = !is_grant && (*stmt).grant_option;
        let privs = render_priv_list((*stmt).privileges);
        if is_grant {
            format!("GRANT {} ON TABLE ", privs)
        } else if revoke_grant_option {
            format!("REVOKE GRANT OPTION FOR {} ON TABLE ", privs)
        } else {
            format!("REVOKE {} ON TABLE ", privs)
        }
    }
}

/// Build the trailing ` TO/FROM <grantees> [WITH GRANT OPTION | CASCADE | RESTRICT]`.
unsafe fn build_grant_suffix(stmt: *mut pg_sys::GrantStmt) -> String {
    unsafe {
        let is_grant = (*stmt).is_grant;
        let grantees = render_role_list((*stmt).grantees);
        let mut s = if is_grant {
            format!(" TO {}", grantees)
        } else {
            format!(" FROM {}", grantees)
        };
        if is_grant && (*stmt).grant_option {
            s.push_str(" WITH GRANT OPTION");
        }
        if !is_grant {
            if (*stmt).behavior == pg_sys::DropBehavior::DROP_CASCADE {
                s.push_str(" CASCADE");
            } else {
                s.push_str(" RESTRICT");
            }
        }
        s
    }
}

/// Render a `List` of `AccessPriv` as a comma-separated SQL fragment.
/// NIL → "ALL".
unsafe fn render_priv_list(privs: *mut pg_sys::List) -> String {
    unsafe {
        if privs.is_null() || (*privs).length == 0 {
            return "ALL".to_string();
        }
        let mut parts: Vec<String> = Vec::new();
        for i in 0..(*privs).length {
            let p = pg_sys::list_nth(privs, i) as *mut pg_sys::AccessPriv;
            if p.is_null() {
                continue;
            }
            let name = cstr_to_string((*p).priv_name);
            if name.is_empty() {
                parts.push("ALL".to_string());
            } else {
                parts.push(name.to_uppercase());
            }
        }
        parts.join(", ")
    }
}

/// Render a `List` of `RoleSpec` as a comma-separated SQL fragment.
unsafe fn render_role_list(roles: *mut pg_sys::List) -> String {
    unsafe {
        if roles.is_null() || (*roles).length == 0 {
            return "PUBLIC".to_string();
        }
        let mut parts: Vec<String> = Vec::new();
        for i in 0..(*roles).length {
            let r = pg_sys::list_nth(roles, i) as *mut pg_sys::RoleSpec;
            parts.push(render_rolespec(r));
        }
        parts.join(", ")
    }
}

/// Classify an `ALTER TABLE ... SET SCHEMA` statement.
pub(crate) unsafe fn handle_alter_object_schema(
    stmt: *mut pg_sys::AlterObjectSchemaStmt,
) -> AlterDisposition {
    unsafe {
        if stmt.is_null() || bypass_active() {
            return AlterDisposition::NotOurTable;
        }
        if (*stmt).objectType != ObjectType::OBJECT_TABLE {
            return AlterDisposition::NotOurTable;
        }
        let target = match lookup_target((*stmt).relation) {
            Some(t) => t,
            None => return AlterDisposition::NotOurTable,
        };
        let new = cstr_to_string((*stmt).newschema);
        AlterDisposition::Tier1 {
            post_actions: vec![PostAction::SetSchema {
                ht_id: target.ht.id,
                new,
            }],
        }
    }
}

/// Per-subcommand verdict (one entry in an `ALTER TABLE … , … , …` chain).
enum SubDisposition {
    Tier1 { post_action: Option<PostAction> },
    Tier3 { op_name: &'static str },
}

/// The classifier — match every `AlterTableType` discriminant and route
/// to Tier 1 or Tier 3. Unrecognized values fall to the `_` arm and are
/// blocked (fail-closed) so new PG versions don't silently allow unsafe
/// operations.
unsafe fn classify_at_subcommand(
    cmd: *mut pg_sys::AlterTableCmd,
    ht: &DeltatableInfo,
) -> SubDisposition {
    let subtype = unsafe { (*cmd).subtype };
    let name = unsafe { cstr_to_string((*cmd).name) };

    match subtype {
        // -------- Pass-through, no catalog work --------
        AlterTableType::AT_DropNotNull
        | AlterTableType::AT_SetStatistics
        | AlterTableType::AT_SetOptions
        | AlterTableType::AT_ResetOptions
        | AlterTableType::AT_ColumnDefault
        | AlterTableType::AT_CookedColumnDefault
        | AlterTableType::AT_DropConstraint
        | AlterTableType::AT_SetRelOptions
        | AlterTableType::AT_ResetRelOptions
        | AlterTableType::AT_ReplaceRelOptions
        | AlterTableType::AT_ReplicaIdentity
        | AlterTableType::AT_GenericOptions
        | AlterTableType::AT_AlterColumnGenericOptions
        | AlterTableType::AT_ReAddComment
        | AlterTableType::AT_SetLogged
        | AlterTableType::AT_SetUnLogged => SubDisposition::Tier1 { post_action: None },

        // -------- Pass-through with companion cascade --------
        // OWNER TO: PG re-owns the parent only; mirror onto every
        // companion table so admins don't have to chase them.
        AlterTableType::AT_ChangeOwner => unsafe {
            let owner_sql = render_rolespec((*cmd).newowner);
            SubDisposition::Tier1 {
                post_action: Some(PostAction::CascadeOwnerToCompanions {
                    ht_id: ht.id,
                    new_owner_sql: owner_sql,
                }),
            }
        },

        // -------- Pass-through with caveats --------
        AlterTableType::AT_EnableTrig
        | AlterTableType::AT_EnableAlwaysTrig
        | AlterTableType::AT_EnableReplicaTrig
        | AlterTableType::AT_EnableTrigUser => SubDisposition::Tier1 { post_action: None },

        AlterTableType::AT_DisableTrig | AlterTableType::AT_DisableTrigUser => {
            if name == "deltax_reject_compressed_dml" {
                SubDisposition::Tier3 {
                    op_name: "DISABLE TRIGGER deltax_reject_compressed_dml \
                              (would allow writes to compressed partitions)",
                }
            } else {
                SubDisposition::Tier1 { post_action: None }
            }
        }

        // DISABLE TRIGGER ALL / ENABLE TRIGGER ALL would silently
        // disable our DML rejection trigger — block both and point
        // the user at the per-trigger form.
        AlterTableType::AT_EnableTrigAll | AlterTableType::AT_DisableTrigAll => {
            SubDisposition::Tier3 {
                op_name: "ENABLE/DISABLE TRIGGER ALL on a deltatable (would affect \
                          pg_deltax's own DML rejection trigger)",
            }
        }

        // ADD INDEX / ADD CONSTRAINT — gate on validation + uniqueness.
        AlterTableType::AT_AddIndex | AlterTableType::AT_ReAddIndex => unsafe {
            classify_add_index(cmd)
        },
        AlterTableType::AT_AddConstraint
        | AlterTableType::AT_ReAddConstraint
        | AlterTableType::AT_AddIndexConstraint => unsafe { classify_add_constraint(cmd) },

        // ADD COLUMN — block volatile defaults / NOT NULL without
        // default / GENERATED / identity. Plain nullable + nonvolatile
        // default pass through; the scan path synthesizes the value at
        // read time via `getmissingattr`.
        AlterTableType::AT_AddColumn | AlterTableType::AT_AddColumnToView => unsafe {
            classify_add_column(cmd)
        },

        // -------- Tier 3: block --------
        AlterTableType::AT_AlterColumnType => SubDisposition::Tier3 {
            op_name: "ALTER COLUMN TYPE",
        },
        AlterTableType::AT_SetStorage => SubDisposition::Tier3 {
            op_name: "ALTER COLUMN SET STORAGE",
        },
        AlterTableType::AT_SetCompression => SubDisposition::Tier3 {
            op_name: "ALTER COLUMN SET COMPRESSION",
        },
        // `AT_CheckNotNull` exists in PG17 (discriminant 8) but was
        // removed in PG18 — the same semantics live under
        // `AT_SetNotNull` there. Gate the extra arm behind pg17 so the
        // PG18 build doesn't see an unresolved constant.
        AlterTableType::AT_SetNotNull => SubDisposition::Tier3 {
            op_name: "ALTER COLUMN SET NOT NULL (would validate against the \
                      empty post-compression heap)",
        },
        #[cfg(feature = "pg17")]
        AlterTableType::AT_CheckNotNull => SubDisposition::Tier3 {
            op_name: "ALTER COLUMN CHECK NOT NULL (would validate against the \
                      empty post-compression heap)",
        },
        AlterTableType::AT_ValidateConstraint => SubDisposition::Tier3 {
            op_name: "VALIDATE CONSTRAINT (would validate against the empty \
                      post-compression heap)",
        },
        AlterTableType::AT_AlterConstraint => SubDisposition::Tier3 {
            op_name: "ALTER CONSTRAINT",
        },
        AlterTableType::AT_AddIdentity
        | AlterTableType::AT_SetIdentity
        | AlterTableType::AT_DropIdentity => SubDisposition::Tier3 {
            op_name: "ADD/SET/DROP IDENTITY",
        },
        AlterTableType::AT_SetExpression | AlterTableType::AT_DropExpression => {
            SubDisposition::Tier3 {
                op_name: "ALTER COLUMN SET/DROP EXPRESSION",
            }
        }
        AlterTableType::AT_EnableRowSecurity
        | AlterTableType::AT_DisableRowSecurity
        | AlterTableType::AT_ForceRowSecurity
        | AlterTableType::AT_NoForceRowSecurity => SubDisposition::Tier3 {
            op_name: "ROW SECURITY policy change",
        },
        AlterTableType::AT_EnableRule
        | AlterTableType::AT_EnableAlwaysRule
        | AlterTableType::AT_EnableReplicaRule
        | AlterTableType::AT_DisableRule => SubDisposition::Tier3 {
            op_name: "ENABLE/DISABLE RULE",
        },
        AlterTableType::AT_ClusterOn | AlterTableType::AT_DropCluster => SubDisposition::Tier3 {
            op_name: "CLUSTER ON / SET WITHOUT CLUSTER",
        },
        AlterTableType::AT_SetAccessMethod => SubDisposition::Tier3 {
            op_name: "SET ACCESS METHOD",
        },
        AlterTableType::AT_SetTableSpace => SubDisposition::Tier3 {
            op_name: "SET TABLESPACE",
        },
        AlterTableType::AT_AddOf | AlterTableType::AT_DropOf => SubDisposition::Tier3 {
            op_name: "ALTER TABLE OF / NOT OF",
        },
        AlterTableType::AT_AttachPartition
        | AlterTableType::AT_DetachPartition
        | AlterTableType::AT_DetachPartitionFinalize => SubDisposition::Tier3 {
            op_name: "ATTACH/DETACH PARTITION (pg_deltax owns partition lifecycle)",
        },
        AlterTableType::AT_AddInherit | AlterTableType::AT_DropInherit => SubDisposition::Tier3 {
            op_name: "INHERIT / NO INHERIT",
        },

        // DROP COLUMN — Tier 2. Non-key columns: pass through and
        // tombstone the descriptor entry post-success. Key columns
        // (segment_by / order_by / time_column) remain Tier 3 because
        // the `_meta` companion embeds them by name.
        AlterTableType::AT_DropColumn => unsafe { classify_drop_column(cmd, ht) },

        // Defensive fall-through: any AT_* discriminant we haven't
        // classified is blocked. Catches new PG versions adding
        // subcommand kinds that might violate the compressed-blob
        // invariants.
        _ => SubDisposition::Tier3 {
            op_name: "<unrecognized ALTER subcommand>",
        },
    }
    .with_context(ht)
}

/// Render a `RoleSpec` as the SQL fragment it represents — `"name"` for
/// a literal role, `CURRENT_USER` / `SESSION_USER` / `CURRENT_ROLE` /
/// `PUBLIC` for the special role keywords. Used to reconstruct the
/// `OWNER TO` clause when cascading to companion tables.
unsafe fn render_rolespec(rv: *mut pg_sys::RoleSpec) -> String {
    unsafe {
        if rv.is_null() {
            return "CURRENT_USER".to_string();
        }
        match (*rv).roletype {
            pg_sys::RoleSpecType::ROLESPEC_CSTRING => {
                let name = cstr_to_string((*rv).rolename);
                // Quote the identifier so role names with mixed case or
                // special chars round-trip correctly.
                format!("\"{}\"", name.replace('"', "\"\""))
            }
            pg_sys::RoleSpecType::ROLESPEC_CURRENT_USER => "CURRENT_USER".to_string(),
            pg_sys::RoleSpecType::ROLESPEC_CURRENT_ROLE => "CURRENT_ROLE".to_string(),
            pg_sys::RoleSpecType::ROLESPEC_SESSION_USER => "SESSION_USER".to_string(),
            pg_sys::RoleSpecType::ROLESPEC_PUBLIC => "PUBLIC".to_string(),
            _ => "CURRENT_USER".to_string(),
        }
    }
}

/// Classify `ADD COLUMN`. Plain nullable + constant-shape default passes
/// through; the scan path synthesizes the missing value at read time via
/// `getmissingattr`. Anything else — volatile default, NOT NULL without
/// default, GENERATED, or identity — is Tier 3.
unsafe fn classify_add_column(cmd: *mut pg_sys::AlterTableCmd) -> SubDisposition {
    unsafe {
        let def = (*cmd).def as *mut pg_sys::ColumnDef;
        if def.is_null() {
            return SubDisposition::Tier1 { post_action: None };
        }

        // PG's parser attaches NOT NULL / DEFAULT / GENERATED / IDENTITY
        // as entries in `ColumnDef.constraints` (a List of Constraint
        // nodes); the `is_not_null` / `raw_default` / `identity` /
        // `generated` fields are populated by
        // `transformColumnDefinition` during analysis, which runs AFTER
        // our ProcessUtility hook fires. So we have to walk the
        // constraints list to know what the user actually wrote.
        let cols = read_column_constraints(def);

        if cols.is_generated || (*def).generated != 0 {
            return SubDisposition::Tier3 {
                op_name: "ADD COLUMN ... GENERATED",
            };
        }
        if cols.is_identity || (*def).identity != 0 {
            return SubDisposition::Tier3 {
                op_name: "ADD COLUMN ... IDENTITY",
            };
        }
        if cols.has_not_null && cols.default_expr.is_null() {
            return SubDisposition::Tier3 {
                op_name: "ADD COLUMN ... NOT NULL without DEFAULT",
            };
        }
        if !cols.default_expr.is_null() && default_is_volatile(cols.default_expr) {
            return SubDisposition::Tier3 {
                op_name: "ADD COLUMN ... DEFAULT <volatile expression>",
            };
        }
        SubDisposition::Tier1 { post_action: None }
    }
}

/// Classify `DROP COLUMN`. Non-key columns are Tier 2: pass through and
/// tombstone the descriptor entry post-success. Key columns
/// (`segment_by` / `order_by` / `time_column`) stay Tier 3 — the `_meta`
/// companion table embeds them by name, and the design doc punts the
/// rewrite to the recipe.
unsafe fn classify_drop_column(
    cmd: *mut pg_sys::AlterTableCmd,
    ht: &DeltatableInfo,
) -> SubDisposition {
    let column_name = unsafe { cstr_to_string((*cmd).name) };
    if column_name.is_empty() {
        // No name on the cmd — defensive; let it fall through to PG.
        return SubDisposition::Tier1 { post_action: None };
    }
    if ht.time_column == column_name
        || ht.segment_by.iter().any(|c| c == &column_name)
        || ht.order_by.iter().any(|c| c == &column_name)
    {
        return SubDisposition::Tier3 {
            op_name: "DROP COLUMN referenced by segment_by / order_by / time_column",
        };
    }
    SubDisposition::Tier1 {
        post_action: Some(PostAction::TombstoneColumn {
            ht_id: ht.id,
            column_name,
        }),
    }
}

/// Parsed view of a `ColumnDef` for `ADD COLUMN` classification.
struct ColumnDefAnalysis {
    has_not_null: bool,
    default_expr: *mut pg_sys::Node,
    is_generated: bool,
    is_identity: bool,
}

/// Walk `ColumnDef.constraints` and surface the bits the classifier
/// needs. The parser stores NOT NULL / DEFAULT / GENERATED / IDENTITY
/// here as `Constraint{contype = CONSTR_*}` entries; the analyzed
/// `is_not_null` / `raw_default` / `generated` / `identity` fields
/// don't get populated until `transformColumnDefinition` runs, which is
/// AFTER our ProcessUtility hook fires.
unsafe fn read_column_constraints(def: *const pg_sys::ColumnDef) -> ColumnDefAnalysis {
    unsafe {
        let mut has_not_null = (*def).is_not_null;
        let mut default_expr: *mut pg_sys::Node = (*def).raw_default;
        if default_expr.is_null() {
            default_expr = (*def).cooked_default;
        }
        let mut is_generated = (*def).generated != 0;
        let mut is_identity = (*def).identity != 0;
        let cons = (*def).constraints;
        if !cons.is_null() {
            let len = (*cons).length;
            for i in 0..len {
                let c = pg_sys::list_nth(cons, i) as *mut pg_sys::Constraint;
                if c.is_null() {
                    continue;
                }
                match (*c).contype {
                    ConstrType::CONSTR_NOTNULL => has_not_null = true,
                    ConstrType::CONSTR_DEFAULT if default_expr.is_null() => {
                        default_expr = (*c).raw_expr;
                    }
                    ConstrType::CONSTR_GENERATED => is_generated = true,
                    ConstrType::CONSTR_IDENTITY => is_identity = true,
                    _ => {}
                }
            }
        }
        ColumnDefAnalysis {
            has_not_null,
            default_expr,
            is_generated,
            is_identity,
        }
    }
}

/// Does the raw `ADD COLUMN ... DEFAULT` expression contain a volatile
/// function? Volatile defaults are unsafe because PG would normally
/// rewrite every existing row evaluating the expression per-row — but
/// pg_deltax partition heaps are empty post-compression, so the rewrite
/// silently no-ops and the column appears unset on compressed rows.
/// Constants, immutable functions, and stable functions (`now()`,
/// `current_date`, etc.) are all fine: PG evaluates them once at ALTER
/// time and stores the result in `pg_attribute.attmissingval`, which
/// the scan path's `getmissingattr` synthesis already reads correctly.
///
/// The check transforms the raw parse-tree default via PG's
/// `transformExpr` (so function references get resolved against
/// `pg_proc`) and then asks `contain_volatile_functions`. If
/// transformation itself errors (e.g. a default referencing an unknown
/// function), that error propagates as the ALTER's failure — same as
/// it would without our hook.
unsafe fn default_is_volatile(raw_default: *mut pg_sys::Node) -> bool {
    unsafe {
        if raw_default.is_null() {
            return false;
        }
        // Build the analysis in a short-lived memory context so all the
        // intermediate allocations (ParseState, transformed Node tree,
        // RTE bookkeeping) are reclaimed in one MemoryContextDelete —
        // without polluting the outer ProcessUtility context that PG's
        // own ALTER analyzer will then run in.
        let mcxt = pg_sys::AllocSetContextCreateInternal(
            pg_sys::CurrentMemoryContext,
            c"deltax_volatile_check".as_ptr(),
            pg_sys::ALLOCSET_SMALL_MINSIZE as usize,
            pg_sys::ALLOCSET_SMALL_INITSIZE as usize,
            pg_sys::ALLOCSET_SMALL_MAXSIZE as usize,
        );
        let old = pg_sys::MemoryContextSwitchTo(mcxt);

        let pstate = pg_sys::make_parsestate(std::ptr::null_mut());
        let analyzed = pg_sys::transformExpr(
            pstate,
            raw_default,
            pg_sys::ParseExprKind::EXPR_KIND_COLUMN_DEFAULT,
        );
        let is_volatile = !analyzed.is_null() && pg_sys::contain_volatile_functions(analyzed);

        pg_sys::MemoryContextSwitchTo(old);
        pg_sys::MemoryContextDelete(mcxt);
        is_volatile
    }
}

/// Classify `ADD INDEX`. Unique indexes can't be validated against the
/// empty post-compression heap, so they're Tier 3.
unsafe fn classify_add_index(cmd: *mut pg_sys::AlterTableCmd) -> SubDisposition {
    unsafe {
        let def = (*cmd).def as *mut pg_sys::IndexStmt;
        if !def.is_null() && (*def).unique {
            SubDisposition::Tier3 {
                op_name: "ADD UNIQUE INDEX",
            }
        } else {
            SubDisposition::Tier1 { post_action: None }
        }
    }
}

/// Classify `ADD CONSTRAINT`. Validating CHECK/FK/PK/UNIQUE/EXCLUDE are
/// Tier 3 (they would validate against the empty post-compression heap).
/// NOT VALID forms and NOT NULL pass through.
unsafe fn classify_add_constraint(cmd: *mut pg_sys::AlterTableCmd) -> SubDisposition {
    unsafe {
        let con = (*cmd).def as *mut pg_sys::Constraint;
        if con.is_null() {
            return SubDisposition::Tier1 { post_action: None };
        }
        let validating = !(*con).skip_validation;
        let contype = (*con).contype;
        let blocking = contype == ConstrType::CONSTR_CHECK
            || contype == ConstrType::CONSTR_FOREIGN
            || contype == ConstrType::CONSTR_PRIMARY
            || contype == ConstrType::CONSTR_UNIQUE
            || contype == ConstrType::CONSTR_EXCLUSION;
        if validating && blocking {
            SubDisposition::Tier3 {
                op_name: "ADD CONSTRAINT (validating form — append NOT VALID for CHECK/FK)",
            }
        } else {
            SubDisposition::Tier1 { post_action: None }
        }
    }
}

impl SubDisposition {
    /// Hook for cross-subcommand context (currently unused; reserved
    /// for future checks like "this subcommand renames a column that's
    /// also being dropped by another subcommand in the same chain").
    #[inline]
    fn with_context(self, _ht: &DeltatableInfo) -> Self {
        self
    }
}
