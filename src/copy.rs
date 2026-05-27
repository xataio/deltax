//! Direct backfill: intercept `COPY ... FROM` with `FORMAT deltax_compress`
//! via a ProcessUtility hook, compress data in-flight, and write directly
//! to companion tables without touching the heap.

use std::ffi::{CStr, CString, c_char};
use std::io::{BufRead, BufReader};
use std::sync::atomic::{AtomicPtr, Ordering};

use pgrx::pg_sys;
use pgrx::pg_sys::ffi::pg_guard_ffi_boundary;
use pgrx::prelude::*;
// SpiClient no longer needed — all SPI calls use short-lived Spi::connect/connect_mut

use crate::catalog;
use crate::compress::{
    ColumnKind, ColumnMeta, PG_EPOCH_OFFSET_USEC, TypedColumn, build_companion_ddl,
    classify_column, compress_text_lengths, compress_typed_column, compute_minmax_encoded_i64,
    compute_segment_blooms, compute_segment_ndistinct, compute_typed_minmax, compute_typed_sum,
    format_minmax_for_insert, get_column_metadata, init_typed_columns, is_text_data_type,
    lz4_clause, new_typed_column, new_worker_typed_column, sort_typed_columns, supports_minmax,
    supports_sum,
};
use crate::copyparse::{
    CopyLineReader, CopyTextOptions, HeaderMode, LineResult, parse_and_append,
    parse_raw_field_and_append, split_field_offsets, split_fields,
};

static PREV_PROCESS_UTILITY_HOOK: AtomicPtr<()> = AtomicPtr::new(std::ptr::null_mut());

/// Register the ProcessUtility hook. Must be called from `_PG_init()`.
///
/// # Safety
/// Must be called exactly once during extension initialization.
pub unsafe fn register_process_utility_hook() {
    unsafe {
        let prev = pg_sys::ProcessUtility_hook;
        if let Some(prev_fn) = prev {
            PREV_PROCESS_UTILITY_HOOK.store(prev_fn as *mut (), Ordering::SeqCst);
        }
        pg_sys::ProcessUtility_hook = Some(deltax_process_utility);
    }
}

/// Chain to the previous ProcessUtility hook, or call standard_ProcessUtility.
#[allow(clippy::too_many_arguments)]
/// Walk a VacuumStmt's `rels` list and remove any entries whose
/// relation is a compressed pg_deltax partition. If the list becomes
/// empty we leave it empty — PG's vacuum executor treats that as "no
/// target", which is the desired outcome.
///
/// This runs at ProcessUtility time for user-initiated
/// `ANALYZE <rel>` / `VACUUM ANALYZE <rel>`. Autovacuum bypasses
/// ProcessUtility entirely, so the `autovacuum_enabled = off` storage
/// option set at compress time is the other half of the belt-and-
/// suspenders protection.
unsafe fn filter_compressed_rels_from_vacuum_stmt(vstmt: *mut pg_sys::VacuumStmt) {
    unsafe {
        if vstmt.is_null() || (*vstmt).rels.is_null() {
            return; // NIL means "all tables"; don't touch that case
        }

        let rels = (*vstmt).rels;
        let len = (*rels).length;
        let mut kept: Vec<*mut std::ffi::c_void> = Vec::with_capacity(len as usize);
        let mut dropped: Vec<String> = Vec::new();

        for i in 0..len {
            let node = pg_sys::list_nth(rels, i) as *mut pg_sys::VacuumRelation;
            if node.is_null() {
                continue;
            }
            let rv = (*node).relation;
            if rv.is_null() {
                kept.push(node as *mut std::ffi::c_void);
                continue;
            }
            // Resolve the RangeVar to an OID without taking a lock; we
            // only need the ID to check the deltax catalog.
            let rel_oid = pg_sys::RangeVarGetRelidExtended(
                rv,
                pg_sys::NoLock as i32,
                pg_sys::RVROption::RVR_MISSING_OK,
                None,
                std::ptr::null_mut(),
            );
            if rel_oid == pg_sys::InvalidOid {
                kept.push(node as *mut std::ffi::c_void);
                continue;
            }
            let companion = crate::scan::check_compressed_partition(rel_oid);
            if companion != pg_sys::InvalidOid {
                let name = std::ffi::CStr::from_ptr((*rv).relname)
                    .to_string_lossy()
                    .into_owned();
                dropped.push(name);
            } else {
                kept.push(node as *mut std::ffi::c_void);
            }
        }

        if dropped.is_empty() {
            return;
        }
        // Rebuild the list with only the kept entries. `list_delete_nth_cell`
        // exists but needs a cell; easier to build a new List here.
        let mut new_list: *mut pg_sys::List = std::ptr::null_mut();
        for n in kept {
            new_list = pg_sys::lappend(new_list, n);
        }
        (*vstmt).rels = new_list;

        pgrx::log!(
            "pg_deltax: skipping ANALYZE on compressed partition(s): {} \
             (stats maintained by deltax_compress_partition; run \
             deltax_analyze_partition() to refresh manually)",
            dropped.join(", "),
        );
    }
}

/// Re-populate `pg_class.reltuples` and `pg_statistic` on every
/// compressed pg_deltax partition after a whole-database
/// `VACUUM ANALYZE` / `ANALYZE` (NIL rels). PG's executor samples the
/// empty heap and overwrites our maintained stats with zeros; invoking
/// `deltax_analyze_partition` recomputes them from the `_colstats`
/// catalog. Idempotent — if PG didn't clobber anything (e.g. `VACUUM`
/// without `ANALYZE`) this just rewrites the same values.
fn restore_compressed_partition_stats() {
    let result: Result<usize, String> = Spi::connect_mut(|client| {
        let rows = client
            .select(
                "SELECT schema_name, table_name FROM deltax.deltax_partition \
                 WHERE is_compressed = true",
                None,
                &[],
            )
            .map_err(|e| format!("failed to list compressed partitions: {}", e))?;

        let partitions: Vec<(String, String)> = rows
            .filter_map(|row| {
                let s: Option<String> = row
                    .get_datum_by_ordinal(1)
                    .ok()
                    .and_then(|d| d.value().ok().flatten());
                let t: Option<String> = row
                    .get_datum_by_ordinal(2)
                    .ok()
                    .and_then(|d| d.value().ok().flatten());
                match (s, t) {
                    (Some(s), Some(t)) => Some((s, t)),
                    _ => None,
                }
            })
            .collect();

        let mut n = 0usize;
        for (s, t) in &partitions {
            // Use the (schema, table) variant to avoid `resolve_relation`'s
            // nested `Spi::get_one_with_args`, which confuses the outer
            // SPI cursor post-VACUUM and surfaces as `InvalidPosition`.
            let _ = crate::compress::analyze_partition_impl_split(client, s, t);
            n += 1;
        }
        Ok::<_, String>(n)
    });
    match result {
        Ok(n) if n > 0 => pgrx::log!(
            "pg_deltax: restored stats on {} compressed partition(s) after VACUUM/ANALYZE",
            n,
        ),
        Ok(_) => {} // no compressed partitions — nothing to do
        Err(e) => pgrx::warning!("pg_deltax: {}", e),
    }
}

#[allow(clippy::too_many_arguments)]
unsafe fn chain_to_prev(
    pstmt: *mut pg_sys::PlannedStmt,
    query_string: *const c_char,
    read_only_tree: bool,
    context: pg_sys::ProcessUtilityContext::Type,
    params: pg_sys::ParamListInfo,
    query_env: *mut pg_sys::QueryEnvironment,
    dest: *mut pg_sys::DestReceiver,
    qc: *mut pg_sys::QueryCompletion,
) {
    unsafe {
        let prev_ptr = PREV_PROCESS_UTILITY_HOOK.load(Ordering::SeqCst);
        if !prev_ptr.is_null() {
            let prev_fn: pg_sys::ProcessUtility_hook_type = Some(std::mem::transmute::<
                *mut (),
                unsafe extern "C-unwind" fn(
                    *mut pg_sys::PlannedStmt,
                    *const c_char,
                    bool,
                    pg_sys::ProcessUtilityContext::Type,
                    pg_sys::ParamListInfo,
                    *mut pg_sys::QueryEnvironment,
                    *mut pg_sys::DestReceiver,
                    *mut pg_sys::QueryCompletion,
                ),
            >(prev_ptr));
            if let Some(f) = prev_fn {
                pg_guard_ffi_boundary(|| {
                    f(
                        pstmt,
                        query_string,
                        read_only_tree,
                        context,
                        params,
                        query_env,
                        dest,
                        qc,
                    )
                });
            }
        } else {
            pg_sys::standard_ProcessUtility(
                pstmt,
                query_string,
                read_only_tree,
                context,
                params,
                query_env,
                dest,
                qc,
            );
        }
    }
}

#[pg_guard]
#[allow(clippy::too_many_arguments)]
unsafe extern "C-unwind" fn deltax_process_utility(
    pstmt: *mut pg_sys::PlannedStmt,
    query_string: *const c_char,
    read_only_tree: bool,
    context: pg_sys::ProcessUtilityContext::Type,
    params: pg_sys::ParamListInfo,
    query_env: *mut pg_sys::QueryEnvironment,
    dest: *mut pg_sys::DestReceiver,
    qc: *mut pg_sys::QueryCompletion,
) {
    let utility_stmt = unsafe { (*pstmt).utilityStmt };

    // Intercept `ANALYZE <compressed_partition>` (and `VACUUM ANALYZE`)
    // before the standard executor samples our empty heap and overwrites
    // the `pg_statistic` rows we maintain ourselves. Compressed
    // partitions are also marked `autovacuum_enabled = off` at compress
    // time; this hook catches user-initiated ANALYZE.
    //
    // When the statement has an explicit `rels` list we filter compressed
    // partitions out directly. When `rels` is NIL (whole-db
    // `VACUUM ANALYZE` / `ANALYZE`) we can't filter — PG expands NIL to
    // every table in its own machinery. Instead we flag the case and
    // restore our stats after the standard executor returns.
    let mut restore_stats_after_vacuum = false;
    if !utility_stmt.is_null() && unsafe { pgrx::is_a(utility_stmt, pg_sys::NodeTag::T_VacuumStmt) }
    {
        unsafe {
            let vstmt = utility_stmt as *mut pg_sys::VacuumStmt;
            if (*vstmt).rels.is_null() {
                restore_stats_after_vacuum = true;
            } else {
                filter_compressed_rels_from_vacuum_stmt(vstmt);
            }
        }
        // Fall through to chain so PG executes the (now-filtered) stmt.
    }

    // Intercept DDL targeting pg_deltax-managed tables (see
    // `src/ddl.rs` and `dev/docs/SCHEMA_CHANGES.md`): classify ALTER
    // subcommands into Tier 1 (transparent, with optional catalog
    // bookkeeping after PG runs the statement), Tier 2 (DROP COLUMN —
    // pass-through with descriptor tombstone), or Tier 3 (block before
    // PG executes).
    if !utility_stmt.is_null() {
        let disposition = unsafe {
            if pgrx::is_a(utility_stmt, pg_sys::NodeTag::T_AlterTableStmt) {
                Some(crate::ddl::handle_alter_table(
                    utility_stmt as *mut pg_sys::AlterTableStmt,
                ))
            } else if pgrx::is_a(utility_stmt, pg_sys::NodeTag::T_RenameStmt) {
                Some(crate::ddl::handle_rename(
                    utility_stmt as *mut pg_sys::RenameStmt,
                ))
            } else if pgrx::is_a(utility_stmt, pg_sys::NodeTag::T_AlterObjectSchemaStmt) {
                Some(crate::ddl::handle_alter_object_schema(
                    utility_stmt as *mut pg_sys::AlterObjectSchemaStmt,
                ))
            } else if pgrx::is_a(utility_stmt, pg_sys::NodeTag::T_GrantStmt) {
                Some(crate::ddl::handle_grant(
                    utility_stmt as *mut pg_sys::GrantStmt,
                ))
            } else {
                None
            }
        };

        match disposition {
            None | Some(crate::ddl::AlterDisposition::NotOurTable) => {}
            Some(crate::ddl::AlterDisposition::Tier3 { op_name, table }) => {
                crate::ddl::raise_tier3(op_name, &table);
            }
            Some(crate::ddl::AlterDisposition::Tier1 { post_actions }) => {
                unsafe {
                    chain_to_prev(
                        pstmt,
                        query_string,
                        read_only_tree,
                        context,
                        params,
                        query_env,
                        dest,
                        qc,
                    );
                }
                crate::ddl::apply_post_actions(post_actions);
                if restore_stats_after_vacuum {
                    restore_compressed_partition_stats();
                }
                return;
            }
        }
    }

    if utility_stmt.is_null() || !unsafe { pgrx::is_a(utility_stmt, pg_sys::NodeTag::T_CopyStmt) } {
        unsafe {
            chain_to_prev(
                pstmt,
                query_string,
                read_only_tree,
                context,
                params,
                query_env,
                dest,
                qc,
            );
        }
        if restore_stats_after_vacuum {
            restore_compressed_partition_stats();
        }
        return;
    }

    let copy_stmt = utility_stmt as *mut pg_sys::CopyStmt;
    let cs = unsafe { &*copy_stmt };

    // Only intercept COPY FROM (not COPY TO)
    if !cs.is_from {
        unsafe {
            chain_to_prev(
                pstmt,
                query_string,
                read_only_tree,
                context,
                params,
                query_env,
                dest,
                qc,
            );
        }
        return;
    }

    // Check for FORMAT deltax_compress / deltax_compress_csv in options
    let (format_idx, is_csv) = find_deltax_format_option(cs.options);
    if format_idx < 0 {
        unsafe {
            chain_to_prev(
                pstmt,
                query_string,
                read_only_tree,
                context,
                params,
                query_env,
                dest,
                qc,
            );
        }
        return;
    }

    // This is our COPY — handle it
    handle_copy_from_deltax_compress(copy_stmt, format_idx, is_csv);

    // Set QueryCompletion to report rows
    if !qc.is_null() {
        unsafe {
            (*qc).commandTag = pg_sys::CommandTag::CMDTAG_COPY;
            (*qc).nprocessed = 0; // updated inside handle_copy_from_deltax_compress via notice
        }
    }
}

/// Walk the options list looking for `FORMAT 'deltax_compress'` or
/// `FORMAT 'deltax_compress_csv'`. Returns `(list_idx, is_csv)` where
/// `is_csv` indicates whether the underlying parser should be CSV-mode
/// (quoted fields) rather than the default TEXT (tab-delimited, backslash
/// escapes). A list_idx of -1 means no match.
fn find_deltax_format_option(options: *mut pg_sys::List) -> (i32, bool) {
    if options.is_null() {
        return (-1, false);
    }
    let list = unsafe { &*options };
    let len = list.length;
    for i in 0..len {
        let cell = unsafe { &*list.elements.add(i as usize) };
        let defelem = unsafe { cell.ptr_value } as *mut pg_sys::DefElem;
        if defelem.is_null() {
            continue;
        }
        let de = unsafe { &*defelem };
        if de.defname.is_null() {
            continue;
        }
        let name = unsafe { CStr::from_ptr(de.defname) };
        if name.to_bytes() != b"format" {
            continue;
        }
        // Get the format value
        if de.arg.is_null() {
            continue;
        }
        let val_str = unsafe { pg_sys::defGetString(defelem) };
        if val_str.is_null() {
            continue;
        }
        let val = unsafe { CStr::from_ptr(val_str) };
        let bytes = val.to_bytes();
        if bytes.eq_ignore_ascii_case(b"deltax_compress") {
            return (i, false);
        }
        if bytes.eq_ignore_ascii_case(b"deltax_compress_csv") {
            return (i, true);
        }
    }
    (-1, false)
}

/// Build a new options list without the FORMAT defelem (so PG defaults to
/// TEXT format for the underlying COPY parser).
unsafe fn strip_format_option(options: *mut pg_sys::List, format_idx: i32) -> *mut pg_sys::List {
    if options.is_null() {
        return std::ptr::null_mut();
    }
    let list = unsafe { &*options };
    let len = list.length;
    let mut new_list: *mut pg_sys::List = std::ptr::null_mut();
    for i in 0..len {
        if i == format_idx {
            continue;
        }
        let cell = unsafe { &*list.elements.add(i as usize) };
        new_list = unsafe { pg_sys::lappend(new_list, cell.ptr_value) };
    }
    new_list
}

/// Build a new options list with the FORMAT defelem at `format_idx` replaced
/// by `FORMAT <new_format>`. Used to swap `deltax_compress_csv` for `csv`
/// before handing the options to PG's `BeginCopyFrom`.
unsafe fn replace_format_option(
    options: *mut pg_sys::List,
    format_idx: i32,
    new_format: &str,
) -> *mut pg_sys::List {
    unsafe {
        let c_format_name = std::ffi::CString::new("format").unwrap();
        let c_format_value = std::ffi::CString::new(new_format).unwrap();
        // Leak the CStrings — PG owns the buffers once they're attached to
        // the DefElem node (the node lives in the current memory context,
        // which PG frees at end of COPY).
        let name_ptr = c_format_name.into_raw();
        let value_ptr = c_format_value.into_raw();
        let value_node = pg_sys::makeString(value_ptr) as *mut pg_sys::Node;
        let new_defelem = pg_sys::makeDefElem(name_ptr, value_node, -1);

        if options.is_null() {
            return pg_sys::lappend(std::ptr::null_mut(), new_defelem as *mut std::ffi::c_void);
        }
        let list = &*options;
        let len = list.length;
        let mut new_list: *mut pg_sys::List = std::ptr::null_mut();
        for i in 0..len {
            if i == format_idx {
                new_list = pg_sys::lappend(new_list, new_defelem as *mut std::ffi::c_void);
            } else {
                let cell = &*list.elements.add(i as usize);
                new_list = pg_sys::lappend(new_list, cell.ptr_value);
            }
        }
        new_list
    }
}

/// Execute SQL via a short-lived SPI connection.
///
/// Each call opens and closes its own `Spi::connect`, so the SPI procedure
/// memory context is freed after every statement. This prevents memory
/// accumulation over thousands of DDL/INSERT calls during a long-running COPY.
fn spi_exec(sql: &str) {
    Spi::connect(|_client| {
        let c_sql = CString::new(sql).expect("SQL string contains null byte");
        let ret = unsafe { pg_sys::SPI_execute(c_sql.as_ptr(), false, 0) };
        if ret < 0 {
            pgrx::error!("SPI_execute failed with code {}", ret);
        }
    });
}

/// Allocate a bytea varlena in `CurrentMemoryContext` and return its Datum.
/// Callers run under a temp context that gets reset/deleted, so the caller
/// doesn't need to pfree explicitly.
fn bytea_to_datum(data: &[u8]) -> pg_sys::Datum {
    unsafe {
        let len = data.len() + pg_sys::VARHDRSZ;
        let varlena = pg_sys::palloc(len) as *mut pg_sys::varlena;
        pgrx::set_varsize_4b(varlena, len as i32);
        let dest = pgrx::vardata_any(varlena as *const pg_sys::varlena) as *mut u8;
        std::ptr::copy_nonoverlapping(data.as_ptr(), dest, data.len());
        pg_sys::Datum::from(varlena as usize)
    }
}

/// Bulk-insert rows into a heap table using `heap_insert` (bypassing SPI).
///
/// Opens `oid` with RowExclusiveLock, creates a `BulkInsertState`, and feeds
/// every item through `build_datums` under a freshly-created temporary
/// memory context that is reset between rows so TOAST scratch allocations
/// don't accumulate. `build_datums` returns one Datum slot per non-null
/// column; this helper assumes no NULL values (all callers insert
/// non-nullable rows). `ctx_name` is the debug name PG attaches to the
/// per-row scratch context — keep it short.
///
/// # Safety
/// Must be called on the PG backend thread (uses table_open/heap_insert).
unsafe fn bulk_heap_insert<T, F>(
    oid: pg_sys::Oid,
    ctx_name: &CStr,
    items: impl IntoIterator<Item = T>,
    build_datums: F,
) where
    F: Fn(&T) -> Vec<pg_sys::Datum>,
{
    unsafe {
        let insert_ctx = pg_sys::AllocSetContextCreateInternal(
            pg_sys::CurrentMemoryContext,
            ctx_name.as_ptr(),
            pg_sys::ALLOCSET_DEFAULT_MINSIZE as usize,
            pg_sys::ALLOCSET_DEFAULT_INITSIZE as usize,
            pg_sys::ALLOCSET_DEFAULT_MAXSIZE as usize,
        );
        let rel = pg_sys::table_open(oid, pg_sys::RowExclusiveLock as pg_sys::LOCKMODE);
        let tupdesc = (*rel).rd_att;
        let bistate = pg_sys::GetBulkInsertState();
        let cid = pg_sys::GetCurrentCommandId(true);

        for item in items {
            let old_ctx = pg_sys::MemoryContextSwitchTo(insert_ctx);
            let mut values = build_datums(&item);
            let mut nulls = vec![false; values.len()];
            let tuple = pg_sys::heap_form_tuple(tupdesc, values.as_mut_ptr(), nulls.as_mut_ptr());
            pg_sys::heap_insert(rel, tuple, cid, 0, bistate);
            pg_sys::heap_freetuple(tuple);
            pg_sys::MemoryContextSwitchTo(old_ctx);
            // Reset frees all TOAST temp allocations + our bytea copy for this row.
            pg_sys::MemoryContextReset(insert_ctx);
        }

        pg_sys::FreeBulkInsertState(bistate);
        pg_sys::table_close(rel, pg_sys::RowExclusiveLock as pg_sys::LOCKMODE);
        pg_sys::MemoryContextDelete(insert_ctx);
    }
}

/// Resolve a fully-qualified table name to its OID via regclass cast.
fn resolve_relation_oid(fqn: &str) -> pg_sys::Oid {
    Spi::connect(|_client| {
        let sql = format!("SELECT '{}'::regclass::oid", fqn);
        let c_sql = CString::new(sql).expect("SQL contains null byte");
        unsafe {
            let ret = pg_sys::SPI_execute(c_sql.as_ptr(), true, 1);
            if ret < 0 || pg_sys::SPI_processed != 1 {
                pgrx::error!("failed to resolve OID for {}", fqn);
            }
            let tuptable = *pg_sys::SPI_tuptable;
            let tupdesc = tuptable.tupdesc;
            let tuple = *tuptable.vals.add(0);
            let mut isnull = false;
            let datum = pg_sys::SPI_getbinval(tuple, tupdesc, 1, &mut isnull);
            if isnull {
                pgrx::error!("NULL OID for {}", fqn);
            }
            pg_sys::Oid::from_u32(datum.value() as u32)
        }
    })
}

/// Resolve a RangeVar to (schema, table) strings.
unsafe fn rangevar_to_names(rv: *const pg_sys::RangeVar) -> (String, String) {
    let rv = unsafe { &*rv };
    let table = unsafe { CStr::from_ptr(rv.relname) }
        .to_str()
        .unwrap()
        .to_string();
    let schema = if rv.schemaname.is_null() {
        // Resolve from search path
        Spi::get_one_with_args::<String>(
            "SELECT schemaname::text FROM pg_tables WHERE tablename = $1::name LIMIT 1",
            &[table.as_str().into()],
        )
        .expect("failed to look up table schema")
        .unwrap_or_else(|| {
            pgrx::error!("pg_deltax: table '{}' not found", table);
        })
    } else {
        unsafe { CStr::from_ptr(rv.schemaname) }
            .to_str()
            .unwrap()
            .to_string()
    };
    (schema, table)
}

/// Maximum blob buffer size per partition before triggering an early flush.
/// Keeps memory bounded even when multiple partitions are active simultaneously.
const BLOB_BUFFER_THRESHOLD: usize = 256 * 1024 * 1024; // 256 MB

/// Number of meta rows to batch into a single multi-row INSERT.
const META_BATCH_SIZE: usize = 50;

/// Per-partition buffer for accumulating rows during direct backfill.
struct PartitionBuffer {
    partition_id: i32,
    partition_schema: String,
    partition_table: String,
    typed_cols: Vec<TypedColumn>,
    row_count: usize,
    next_segment_id: i32,
    blob_buffer: Vec<(u16, i32, Vec<u8>)>,
    blob_buffer_size: usize,
    bloom_buffer: Vec<(u16, i32, u8, Vec<u8>)>,
    /// Accumulated text-length sidecar blobs (col_idx, segment_id, length_blob).
    text_length_buffer: Vec<(u16, i32, Vec<u8>)>,
    /// Per-segment distinct-value sets for low-cardinality text columns.
    /// Encoded into bitmaps in `finalize_partition` once the partition-level
    /// value→bit_idx map is known.
    valbitmap_value_buffer: Vec<(u16, i32, Vec<String>)>,
    total_compressed_size: i64,
    total_rows: i64,
    meta_table_created: bool,
    blobs_table_created: bool,
    blobs_flushed: bool,
    /// Cached meta table FQN and column list for batched INSERTs.
    meta_fqn: Option<String>,
    meta_insert_cols: Option<String>,
    /// Buffered VALUES clauses for batched meta INSERTs.
    meta_insert_rows: Vec<String>,
    /// Cached colstats table FQN for batched INSERTs.
    colstats_fqn: Option<String>,
    /// Buffered VALUES clauses for batched colstats INSERTs.
    colstats_insert_rows: Vec<String>,
    /// Cached companion table FQNs and OIDs to avoid repeated SPI lookups.
    blobs_fqn_cached: Option<String>,
    blooms_fqn_cached: Option<String>,
    blobs_oid_cached: Option<pg_sys::Oid>,
    blooms_oid_cached: Option<pg_sys::Oid>,
    /// Cached text_lengths FQN/OID, created lazily on first flush.
    text_lengths_fqn_cached: Option<String>,
    text_lengths_oid_cached: Option<pg_sys::Oid>,
    text_lengths_table_created: bool,
}

/// State for the entire backfill operation.
struct BackfillState {
    columns: Vec<ColumnMeta>,
    kinds: Vec<ColumnKind>,
    order_col_indices: Vec<usize>,
    segment_size: usize,
    time_col_index: usize,
    /// `extract_targets[i]` is the JSON-extraction config for physical column
    /// `i`, or `None` if column `i` has no extracted children. Indexed by
    /// physical column position; entries beyond physical columns are absent.
    /// Empty Vec when no json_extract is configured.
    extract_targets: Vec<Option<crate::compress::ColumnExtractTargets>>,
}

impl BackfillState {
    /// Number of physical columns (i.e. TSV/COPY field count). Excludes
    /// synthetic extracted columns appended at the end of `columns`.
    fn physical_column_count(&self) -> usize {
        self.columns
            .iter()
            .position(|c| c.extracted.is_some())
            .unwrap_or(self.columns.len())
    }
}

/// Companion blobs-table DDL (no PK; PK added in `finalize_partition`).
fn create_blobs_table(fqn: &str) {
    spi_exec(&format!(
        "CREATE TABLE {} (_col_idx SMALLINT NOT NULL, _segment_id INT NOT NULL, _data BYTEA{})",
        fqn,
        lz4_clause()
    ));
}

/// Companion blooms-table DDL (no PK; PK added in `finalize_partition`).
fn create_blooms_table(fqn: &str) {
    spi_exec(&format!(
        "CREATE TABLE {} (_col_idx SMALLINT NOT NULL, _segment_id INT NOT NULL, _num_hashes SMALLINT NOT NULL, _data BYTEA{} NOT NULL)",
        fqn,
        lz4_clause()
    ));
}

/// Companion text-lengths-table DDL (no PK; PK added in `finalize_partition`).
fn create_text_lengths_table(fqn: &str) {
    spi_exec(&format!(
        "CREATE TABLE {} (_col_idx SMALLINT NOT NULL, _segment_id INT NOT NULL, _data BYTEA{} NOT NULL)",
        fqn,
        lz4_clause()
    ));
}

impl PartitionBuffer {
    /// Cache the meta/colstats FQNs and the meta INSERT column list. Idempotent;
    /// the column list is constant for a partition so building it once amortises
    /// over every segment flush.
    fn cache_companion_fqns(&mut self, columns: &[ColumnMeta]) {
        if self.meta_fqn.is_some() && self.colstats_fqn.is_some() {
            return;
        }
        let ddl = build_companion_ddl(&self.partition_table, columns);
        if self.meta_fqn.is_none() {
            self.meta_fqn = Some(ddl.meta_fqn);
            let mut cols: Vec<String> = vec!["_segment_id".to_string()];
            for col in columns {
                if col.is_segment_by {
                    cols.push(format!("\"{}\"", col.name));
                }
            }
            for col in columns {
                if col.is_time_column && !col.is_segment_by && supports_minmax(&col.data_type) {
                    cols.push(format!("\"_min_{}\"", col.name));
                    cols.push(format!("\"_max_{}\"", col.name));
                }
            }
            cols.push("_row_count".to_string());
            self.meta_insert_cols = Some(cols.join(", "));
        }
        if self.colstats_fqn.is_none() {
            self.colstats_fqn = Some(ddl.colstats_fqn);
        }
    }
}

fn handle_copy_from_deltax_compress(
    copy_stmt: *mut pg_sys::CopyStmt,
    format_idx: i32,
    is_csv: bool,
) {
    // Bypass DML-on-compressed check for our companion table writes
    crate::scan::set_dml_bypass(true);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        handle_copy_from_inner(copy_stmt, format_idx, is_csv);
    }));
    crate::scan::set_dml_bypass(false);
    if let Err(e) = result {
        std::panic::resume_unwind(e);
    }
}

/// Extract COPY TEXT options (DELIMITER, NULL, HEADER) from the PG options list.
fn extract_copy_text_options(options: *mut pg_sys::List, format_idx: i32) -> CopyTextOptions {
    let mut opts = CopyTextOptions::default();
    if options.is_null() {
        return opts;
    }
    let list = unsafe { &*options };
    let len = list.length;
    for i in 0..len {
        if i == format_idx {
            continue;
        }
        let cell = unsafe { &*list.elements.add(i as usize) };
        let defelem = unsafe { cell.ptr_value } as *mut pg_sys::DefElem;
        if defelem.is_null() {
            continue;
        }
        let de = unsafe { &*defelem };
        if de.defname.is_null() {
            continue;
        }
        let name = unsafe { CStr::from_ptr(de.defname) };
        let name_bytes = name.to_bytes();
        if name_bytes.eq_ignore_ascii_case(b"delimiter") {
            let val_str = unsafe { pg_sys::defGetString(defelem) };
            if !val_str.is_null() {
                let val = unsafe { CStr::from_ptr(val_str) };
                let bytes = val.to_bytes();
                if !bytes.is_empty() {
                    opts.delimiter = bytes[0];
                }
            }
        } else if name_bytes.eq_ignore_ascii_case(b"null") {
            let val_str = unsafe { pg_sys::defGetString(defelem) };
            if !val_str.is_null() {
                let val = unsafe { CStr::from_ptr(val_str) };
                opts.null_string = val.to_bytes().to_vec();
            }
        } else if name_bytes.eq_ignore_ascii_case(b"header") {
            // HEADER can be boolean (true/false) or 'match'
            let val_str = unsafe { pg_sys::defGetString(defelem) };
            if !val_str.is_null() {
                let val = unsafe { CStr::from_ptr(val_str) };
                let val_bytes = val.to_bytes();
                if val_bytes.eq_ignore_ascii_case(b"match") {
                    opts.header = HeaderMode::Match(Vec::new());
                } else if val_bytes.eq_ignore_ascii_case(b"true")
                    || val_bytes.eq_ignore_ascii_case(b"on")
                    || val_bytes == b"1"
                {
                    opts.header = HeaderMode::Skip;
                }
                // false/off/0 → HeaderMode::None (default)
            }
        }
    }
    opts
}

fn handle_copy_from_inner(copy_stmt: *mut pg_sys::CopyStmt, format_idx: i32, is_csv: bool) {
    let cs = unsafe { &*copy_stmt };

    // 1. Resolve table
    if cs.relation.is_null() {
        pgrx::error!("pg_deltax: COPY FROM with FORMAT deltax_compress requires a relation");
    }
    let (schema, table) = unsafe { rangevar_to_names(cs.relation) };

    // 2. Validate via SPI — use a short-lived connection so its memory context
    //    is freed before the long-running COPY loop starts.
    let (partitions, columns, kinds, time_col_index, order_col_indices, segment_size) =
        Spi::connect_mut(|client| {
            let ht = catalog::get_deltatable(client, &schema, &table)
                .expect("failed to query deltatable")
                .unwrap_or_else(|| {
                    pgrx::error!(
                        "pg_deltax: {}.{} is not a deltax table. Call deltax_create_table() first.",
                        schema,
                        table
                    );
                });

            if ht.order_by.is_empty() && ht.segment_by.is_empty() {
                pgrx::error!(
                    "pg_deltax: compression not enabled on {}.{}. Call deltax_enable_compression() first.",
                    schema,
                    table
                );
            }

            // 3. Load partitions
            let partitions =
                catalog::get_partitions(client, ht.id).expect("failed to query partitions");

            if partitions.is_empty() {
                pgrx::error!("pg_deltax: no partitions found for {}.{}", schema, table);
            }

            // Get column metadata from the parent table, with any json_extract
            // synthetic columns appended at the end.
            let columns = get_column_metadata(
                client,
                &schema,
                &table,
                &ht.segment_by,
                &ht.time_column,
                ht.json_extract.as_ref(),
            );
            if columns.is_empty() {
                pgrx::error!("pg_deltax: no columns found for {}.{}", schema, table);
            }

            let kinds: Vec<ColumnKind> = columns
                .iter()
                .map(|c| classify_column(&c.data_type, c.is_segment_by))
                .collect();

            // Find the time column index
            let time_col_index = columns
                .iter()
                .position(|c| c.name == ht.time_column)
                .unwrap_or_else(|| {
                    pgrx::error!(
                        "pg_deltax: time column '{}' not found in column metadata",
                        ht.time_column
                    );
                });
            // Build order_by column indices
            let order_col_indices: Vec<usize> = ht
                .order_by
                .iter()
                .filter_map(|name| columns.iter().position(|c| c.name == *name))
                .collect();

            let segment_size = ht.segment_size as usize;

            (
                partitions,
                columns,
                kinds,
                time_col_index,
                order_col_indices,
                segment_size,
            )
        });
    // SPI connection is now closed — its memory context has been freed.

    // Build partition range arrays (in Unix epoch usec) for binary search
    let mut range_starts: Vec<i64> = Vec::with_capacity(partitions.len());
    let mut range_ends: Vec<i64> = Vec::with_capacity(partitions.len());
    let mut part_buffers: Vec<PartitionBuffer> = Vec::with_capacity(partitions.len());

    for p in &partitions {
        let start_usec = p.range_start.into_inner() + PG_EPOCH_OFFSET_USEC;
        let end_usec = p.range_end.into_inner() + PG_EPOCH_OFFSET_USEC;
        range_starts.push(start_usec);
        range_ends.push(end_usec);
        part_buffers.push(PartitionBuffer {
            partition_id: p.id,
            partition_schema: p.schema_name.clone(),
            partition_table: p.table_name.clone(),
            typed_cols: init_typed_columns(&columns, &kinds),
            row_count: 0,
            next_segment_id: 1,
            blob_buffer: Vec::new(),
            blob_buffer_size: 0,
            bloom_buffer: Vec::new(),
            text_length_buffer: Vec::new(),
            valbitmap_value_buffer: Vec::new(),
            total_compressed_size: 0,
            total_rows: 0,
            meta_table_created: false,
            blobs_table_created: false,
            blobs_flushed: false,
            meta_fqn: None,
            meta_insert_cols: None,
            meta_insert_rows: Vec::new(),
            colstats_fqn: None,
            colstats_insert_rows: Vec::new(),
            blobs_fqn_cached: None,
            blooms_fqn_cached: None,
            blobs_oid_cached: None,
            blooms_oid_cached: None,
            text_lengths_fqn_cached: None,
            text_lengths_oid_cached: None,
            text_lengths_table_created: false,
        });
    }

    let extract_targets = crate::compress::build_extract_targets_per_column(&columns);

    let state = BackfillState {
        columns,
        kinds,
        order_col_indices,
        segment_size,
        time_col_index,
        extract_targets,
    };

    // Branch: file-path → pure-Rust parser, stdin → legacy PG parser.
    // CSV variant always routes through legacy (BeginCopyFrom supports CSV
    // quoting; the fast Rust parser is TEXT-format only).
    if !cs.filename.is_null() && !cs.is_program && !is_csv {
        let filename = unsafe { CStr::from_ptr(cs.filename) }
            .to_str()
            .unwrap_or_else(|_| pgrx::error!("pg_deltax: filename is not valid UTF-8"));
        let files = expand_file_glob(filename);
        let is_parquet = files[0].ends_with(".parquet") || files[0].ends_with(".parq");

        if is_parquet {
            let n_workers = crate::get_parallel_workers();
            if n_workers <= 1 || files.len() <= 1 {
                for file in &files {
                    handle_copy_from_parquet(
                        file,
                        &state,
                        &mut part_buffers,
                        &range_starts,
                        &range_ends,
                    );
                }
            } else {
                handle_copy_from_parquet_parallel(
                    &files,
                    &state,
                    &mut part_buffers,
                    &range_starts,
                    &range_ends,
                );
            }
        } else {
            // TSV path: loop over globbed files within a single COPY so
            // partition finalization happens once at the end (same as parquet).
            // Per-file COPYs would mark partitions compressed after the first
            // file and reject subsequent loads into the same time range.
            let copy_opts = extract_copy_text_options(cs.options, format_idx);
            for file in &files {
                handle_copy_from_file(
                    file,
                    copy_opts.clone(),
                    &state,
                    &mut part_buffers,
                    &partitions,
                    &range_starts,
                    &range_ends,
                );
            }
        }
    } else {
        handle_copy_from_legacy(
            cs,
            format_idx,
            is_csv,
            &state,
            &mut part_buffers,
            &partitions,
            &range_starts,
            &range_ends,
        );
    }

    // End-of-COPY flush (shared by both paths)
    for buf in &mut part_buffers {
        if buf.row_count == 0 && buf.total_rows == 0 {
            continue;
        }

        // Flush remaining partial segment
        if buf.row_count > 0 {
            flush_segment(buf, &state);
        }

        // Flush any remaining buffered meta rows
        flush_meta_buffer(buf);

        // Flush any remaining blobs (may already be flushed via partition-change logic)
        if !buf.blob_buffer.is_empty() {
            flush_partition_blobs(buf, &state.columns);
        }

        // ANALYZE companion tables and update catalog
        finalize_partition(buf, &state.columns);
    }

    crate::scan::invalidate_compressed_cache();

    let total_rows: i64 = part_buffers.iter().map(|b| b.total_rows).sum();
    pgrx::notice!(
        "pg_deltax: direct backfill complete, {} rows compressed into {} partitions",
        total_rows,
        part_buffers.iter().filter(|b| b.total_rows > 0).count()
    );
}

// ============================================================================
// Parallel chunk parsing types
// ============================================================================

struct WorkerPartitionResult {
    typed_cols: Vec<TypedColumn>,
    row_count: usize,
}

struct WorkerResult {
    partitions: Vec<Option<WorkerPartitionResult>>,
}

/// Worker function: parse a slice of lines into per-partition TypedColumn buffers.
/// Pure Rust — no pgrx calls (workers can't access the PG backend).
#[allow(clippy::too_many_arguments)]
fn parse_lines_worker(
    buf: &[u8],
    line_ranges: &[(usize, usize)],
    opts_delimiter: u8,
    opts_null_string: &[u8],
    kinds: &[ColumnKind],
    extract_targets: &[Option<crate::compress::ColumnExtractTargets>],
    time_col_index: usize,
    range_starts: &[i64],
    range_ends: &[i64],
    is_compressed: &[bool],
    n_partitions: usize,
    base_line_number: u64,
) -> Result<WorkerResult, crate::copyparse::ParseError> {
    // `kinds.len()` covers physical + extracted columns (extracted sit at the
    // tail). `physical_count` is the number of TSV fields per row, and is
    // also the prefix of `kinds` we drive through the parser.
    let physical_count = if extract_targets.is_empty() {
        kinds.len()
    } else {
        extract_targets.len()
    };
    let total_columns = kinds.len();
    let mut partitions: Vec<Option<WorkerPartitionResult>> =
        (0..n_partitions).map(|_| None).collect();
    let mut field_offsets: Vec<(usize, usize)> = Vec::with_capacity(physical_count);

    for (row_idx, &(s, e)) in line_ranges.iter().enumerate() {
        let line_number = base_line_number + row_idx as u64 + 1;
        let line = &buf[s..e];
        split_field_offsets(line, opts_delimiter, &mut field_offsets);

        if field_offsets.len() != physical_count {
            return Err(crate::copyparse::ParseError {
                message: format!(
                    "expected {} fields, got {}",
                    physical_count,
                    field_offsets.len()
                ),
                column: 0,
                line: line_number,
            });
        }

        // Extract time value
        let (ts, te) = field_offsets[time_col_index];
        let time_raw = &line[ts..te];
        if time_raw == opts_null_string {
            return Err(crate::copyparse::ParseError {
                message: "time column value is NULL, cannot route to partition".to_string(),
                column: time_col_index,
                line: line_number,
            });
        }
        let time_str = if memchr::memchr(b'\\', time_raw).is_none() {
            std::str::from_utf8(time_raw).map_err(|_| crate::copyparse::ParseError {
                message: "invalid UTF-8 in time column".to_string(),
                column: time_col_index,
                line: line_number,
            })?
        } else {
            return Err(crate::copyparse::ParseError {
                message: "unexpected escape in time column".to_string(),
                column: time_col_index,
                line: line_number,
            });
        };
        let time_usec = crate::timeparse::parse_timestamp_to_usec(time_str);

        let part_idx = match find_partition(range_starts, range_ends, time_usec) {
            Some(idx) => idx,
            None => {
                return Err(crate::copyparse::ParseError {
                    message: format!("timestamp {} does not fit any partition", time_usec),
                    column: time_col_index,
                    line: line_number,
                });
            }
        };

        if is_compressed[part_idx] {
            return Err(crate::copyparse::ParseError {
                message: "partition is already compressed. Decompress it first to load new data."
                    .to_string(),
                column: 0,
                line: line_number,
            });
        }

        // Lazily initialize partition buffers. Workers cannot call into PG
        // (jsonb_in is not thread-safe), so JSONB columns accumulate as Text
        // and the merge phase converts them to binary on the main thread.
        let wp = partitions[part_idx].get_or_insert_with(|| {
            let typed_cols: Vec<TypedColumn> =
                kinds.iter().map(|k| new_worker_typed_column(*k)).collect();
            WorkerPartitionResult {
                typed_cols,
                row_count: 0,
            }
        });

        // Parse each physical field into the partition's typed columns. Then
        // run JSON-path extraction for any source columns that have specs —
        // this populates the synthetic extracted columns at typed_cols
        // positions [physical_count..total_columns).
        for (i, kind) in kinds.iter().take(physical_count).enumerate() {
            let (fs, fe) = field_offsets[i];
            let raw_field = &line[fs..fe];
            parse_raw_field_and_append(
                raw_field,
                opts_null_string,
                *kind,
                &mut wp.typed_cols[i],
                i,
                line_number,
            )?;
        }
        for (i, targets) in extract_targets.iter().enumerate() {
            let Some(targets) = targets else { continue };
            let (fs, fe) = field_offsets[i];
            let raw_field = &line[fs..fe];
            crate::compress::extract_from_raw_field(
                raw_field,
                opts_null_string,
                targets,
                &mut wp.typed_cols,
            );
        }
        wp.row_count += 1;
        // total_columns sanity (silences unused warning when extract is empty)
        let _ = total_columns;
    }

    Ok(WorkerResult { partitions })
}

/// Pure-Rust file-path COPY: read the file directly, parse TEXT format,
/// convert types, and route to partition buffers.
fn handle_copy_from_file(
    filename: &str,
    opts: CopyTextOptions,
    state: &BackfillState,
    part_buffers: &mut [PartitionBuffer],
    partitions: &[crate::catalog::PartitionInfo],
    range_starts: &[i64],
    range_ends: &[i64],
) {
    let n_workers = crate::get_parallel_workers();
    if n_workers <= 1 {
        return handle_copy_from_file_sequential(
            filename,
            opts,
            state,
            part_buffers,
            partitions,
            range_starts,
            range_ends,
        );
    }

    let file = std::fs::File::open(filename).unwrap_or_else(|e| {
        pgrx::error!("pg_deltax: cannot open file '{}': {}", filename, e);
    });
    let mut reader = BufReader::with_capacity(128 * 1024 * 1024, file);
    let mut line_reader = CopyLineReader::new();
    let mut buf: Vec<u8> = Vec::with_capacity(160 * 1024 * 1024);

    let mut total_rows: i64 = 0;
    let mut last_part_idx: Option<usize> = None;
    let mut parse_time_us: u64 = 0;
    let copy_start = std::time::Instant::now();

    // Initial fill
    {
        let data = reader.fill_buf().unwrap_or_else(|e| {
            pgrx::error!("pg_deltax: read error: {}", e);
        });
        buf.extend_from_slice(data);
        let n = data.len();
        reader.consume(n);
    }

    // Handle HEADER
    if matches!(opts.header, HeaderMode::Skip | HeaderMode::Match(_)) {
        match line_reader.next_line(&buf, 0) {
            LineResult::Row(_, e) => {
                let eol_len = match line_reader.eol {
                    Some(crate::copyparse::Eol::CrLf) => 2,
                    _ => 1,
                };
                buf.drain(..e + eol_len);
            }
            LineResult::EndOfCopy => {
                return;
            }
            LineResult::Incomplete => {
                pgrx::error!("pg_deltax: file has no complete header line");
            }
        }
    }

    let num_columns = state.physical_column_count();
    let is_compressed: Vec<bool> = partitions.iter().map(|p| p.is_compressed).collect();
    let n_partitions = part_buffers.len();

    pgrx::notice!("pg_deltax: parallel COPY with {} workers", n_workers);

    loop {
        // Phase 1: Find all line boundaries (sequential, memchr-fast)
        let t_parse = std::time::Instant::now();
        let mut line_ranges: Vec<(usize, usize)> = Vec::new();
        let mut pos: usize = 0;
        let mut end_of_copy = false;

        loop {
            match line_reader.next_line(&buf, pos) {
                LineResult::Row(s, e) => {
                    line_ranges.push((s, e));
                    let eol_len = match line_reader.eol {
                        Some(crate::copyparse::Eol::CrLf) => 2,
                        _ => 1,
                    };
                    pos = e + eol_len;
                }
                LineResult::EndOfCopy => {
                    end_of_copy = true;
                    break;
                }
                LineResult::Incomplete => break,
            }
        }

        if line_ranges.is_empty() {
            if end_of_copy {
                break;
            }
            // Need more data (line spans batch boundary)
            let data = reader.fill_buf().unwrap_or_else(|e| {
                pgrx::error!("pg_deltax: read error: {}", e);
            });
            if data.is_empty() {
                // EOF — handle trailing line without terminator
                if !buf.is_empty() {
                    let line = &buf[..];
                    let raw_fields = split_fields(line, opts.delimiter);
                    if raw_fields.len() == num_columns {
                        line_reader.line_number += 1;
                        handle_trailing_line(
                            &raw_fields,
                            &opts,
                            state,
                            part_buffers,
                            partitions,
                            range_starts,
                            range_ends,
                            &mut last_part_idx,
                            &mut total_rows,
                            line_reader.line_number,
                        );
                    }
                }
                break;
            }
            buf.extend_from_slice(data);
            let n = data.len();
            reader.consume(n);
            continue;
        }

        let scan_time = t_parse.elapsed().as_micros() as u64;

        // Phase 2: Parallel parse
        let t_parallel = std::time::Instant::now();
        let chunk_size = line_ranges.len().div_ceil(n_workers);
        let base_line = line_reader.line_number - line_ranges.len() as u64;

        let buf_ref = &buf;
        let null_string_ref = &opts.null_string;
        let kinds_ref = &state.kinds;
        let extract_targets_ref = &state.extract_targets;
        let is_compressed_ref = &is_compressed;
        let delimiter = opts.delimiter;
        let time_col_index = state.time_col_index;

        let worker_results: Vec<Result<WorkerResult, crate::copyparse::ParseError>> =
            std::thread::scope(|s| {
                line_ranges
                    .chunks(chunk_size)
                    .enumerate()
                    .map(|(chunk_idx, chunk)| {
                        let chunk_base_line = base_line + (chunk_idx * chunk_size) as u64;
                        s.spawn(move || {
                            parse_lines_worker(
                                buf_ref,
                                chunk,
                                delimiter,
                                null_string_ref,
                                kinds_ref,
                                extract_targets_ref,
                                time_col_index,
                                range_starts,
                                range_ends,
                                is_compressed_ref,
                                n_partitions,
                                chunk_base_line,
                            )
                        })
                    })
                    .collect::<Vec<_>>()
                    .into_iter()
                    .map(|h| {
                        h.join().unwrap_or_else(|payload| {
                            // Surface the actual panic message rather than the
                            // opaque `Any { .. }` Display impl on Box<dyn Any>.
                            let msg = if let Some(s) = payload.downcast_ref::<&str>() {
                                (*s).to_string()
                            } else if let Some(s) = payload.downcast_ref::<String>() {
                                s.clone()
                            } else {
                                "unknown panic payload".to_string()
                            };
                            Err(crate::copyparse::ParseError {
                                message: format!("worker thread panicked: {}", msg),
                                column: 0,
                                line: 0,
                            })
                        })
                    })
                    .collect()
            });

        parse_time_us += scan_time + t_parallel.elapsed().as_micros() as u64;

        // Phase 3: Merge + flush (sequential, on PG backend thread)
        merge_and_flush_results(worker_results, part_buffers, state, &mut total_rows);

        // Drain consumed bytes
        if pos > 0 {
            buf.drain(..pos);
        }

        if end_of_copy {
            break;
        }

        // Read more data
        let data = reader.fill_buf().unwrap_or_else(|e| {
            pgrx::error!("pg_deltax: read error: {}", e);
        });
        if data.is_empty() {
            // EOF — handle trailing partial line
            if !buf.is_empty() {
                let line = &buf[..];
                let raw_fields = split_fields(line, opts.delimiter);
                if raw_fields.len() == num_columns {
                    line_reader.line_number += 1;
                    handle_trailing_line(
                        &raw_fields,
                        &opts,
                        state,
                        part_buffers,
                        partitions,
                        range_starts,
                        range_ends,
                        &mut last_part_idx,
                        &mut total_rows,
                        line_reader.line_number,
                    );
                }
            }
            break;
        }
        buf.extend_from_slice(data);
        let n = data.len();
        reader.consume(n);
    }

    let copy_elapsed = copy_start.elapsed();
    pgrx::notice!(
        "pg_deltax: COPY (Rust parser, {} workers) done: {} rows in {:.1}s, parse={:.1}s ({:.0}%)",
        n_workers,
        total_rows,
        copy_elapsed.as_secs_f64(),
        parse_time_us as f64 / 1e6,
        if copy_elapsed.as_secs_f64() > 0.0 {
            (parse_time_us as f64 / 1e6) / copy_elapsed.as_secs_f64() * 100.0
        } else {
            0.0
        }
    );
}

/// Handle a trailing line at EOF (no terminator). Used by both parallel and sequential paths.
#[allow(clippy::too_many_arguments)]
fn handle_trailing_line(
    raw_fields: &[&[u8]],
    opts: &CopyTextOptions,
    state: &BackfillState,
    part_buffers: &mut [PartitionBuffer],
    partitions: &[crate::catalog::PartitionInfo],
    range_starts: &[i64],
    range_ends: &[i64],
    last_part_idx: &mut Option<usize>,
    total_rows: &mut i64,
    line_number: u64,
) {
    let time_raw = raw_fields[state.time_col_index];
    if time_raw == opts.null_string.as_slice() {
        pgrx::error!(
            "pg_deltax: time column value is NULL at line {}",
            line_number
        );
    }
    let time_str = std::str::from_utf8(time_raw).unwrap_or_else(|_| {
        pgrx::error!("pg_deltax: invalid UTF-8 in time column");
    });
    let time_usec = crate::timeparse::parse_timestamp_to_usec(time_str);

    let part_idx = match find_partition(range_starts, range_ends, time_usec) {
        Some(idx) => idx,
        None => {
            pgrx::error!(
                "pg_deltax: row at line {} with timestamp {} does not fit any partition",
                line_number,
                time_usec
            );
        }
    };

    if partitions[part_idx].is_compressed {
        pgrx::error!(
            "pg_deltax: partition '{}' is already compressed.",
            partitions[part_idx].table_name
        );
    }

    if let Some(prev_idx) =
        last_part_idx.filter(|&idx| idx != part_idx && !part_buffers[idx].blob_buffer.is_empty())
    {
        flush_partition_blobs(&mut part_buffers[prev_idx], &state.columns);
    }

    let pbuf = &mut part_buffers[part_idx];
    let physical_count = state.physical_column_count();
    for (i, (raw_field, kind)) in raw_fields
        .iter()
        .zip(state.kinds.iter())
        .take(physical_count)
        .enumerate()
    {
        if let Err(e) = parse_raw_field_and_append(
            raw_field,
            &opts.null_string,
            *kind,
            &mut pbuf.typed_cols[i],
            i,
            line_number,
        ) {
            pgrx::error!(
                "pg_deltax: parse error at line {}, column {}: {}",
                e.line,
                e.column,
                e.message
            );
        }
    }
    // Apply JSON-path extraction for any source column with specs.
    for (i, targets) in state.extract_targets.iter().enumerate() {
        let Some(targets) = targets else { continue };
        if i < raw_fields.len() {
            crate::compress::extract_from_raw_field(
                raw_fields[i],
                &opts.null_string,
                targets,
                &mut pbuf.typed_cols,
            );
        }
    }
    pbuf.row_count += 1;
    *total_rows += 1;

    if pbuf.row_count >= state.segment_size {
        flush_segment(pbuf, state);
    }
}

/// Merge worker results into partition buffers, flushing segments as they fill.
fn merge_and_flush_results(
    worker_results: Vec<Result<WorkerResult, crate::copyparse::ParseError>>,
    part_buffers: &mut [PartitionBuffer],
    state: &BackfillState,
    total_rows: &mut i64,
) {
    for result in worker_results {
        let result = match result {
            Ok(r) => r,
            Err(e) => {
                pgrx::error!(
                    "pg_deltax: parse error at line {}, column {}: {}",
                    e.line,
                    e.column,
                    e.message
                );
            }
        };

        for (part_idx, worker_part) in result.partitions.into_iter().enumerate() {
            if let Some(wp) = worker_part {
                let pbuf = &mut part_buffers[part_idx];
                for (i, worker_col) in wp.typed_cols.into_iter().enumerate() {
                    // JSONB came back from the worker as raw text; convert to
                    // the binary jsonb varlena now (we're back on the PG
                    // backend thread, so jsonb_in is safe to call).
                    let merged = match (state.kinds[i], worker_col) {
                        (ColumnKind::Jsonb, TypedColumn::Text(texts)) => {
                            let bytes: Vec<Option<Vec<u8>>> = texts
                                .into_iter()
                                .map(|opt| {
                                    opt.map(|s| unsafe {
                                        crate::compress::jsonb_text_to_binary(&s)
                                    })
                                })
                                .collect();
                            TypedColumn::Bytes(bytes)
                        }
                        (_, other) => other,
                    };
                    pbuf.typed_cols[i].extend(merged);
                }
                pbuf.row_count += wp.row_count;
                *total_rows += wp.row_count as i64;

                if pbuf.row_count >= state.segment_size {
                    flush_segment(pbuf, state);
                }
            }
        }
    }
}

/// Sequential file-path COPY (fallback for single-worker mode).
fn handle_copy_from_file_sequential(
    filename: &str,
    opts: CopyTextOptions,
    state: &BackfillState,
    part_buffers: &mut [PartitionBuffer],
    partitions: &[crate::catalog::PartitionInfo],
    range_starts: &[i64],
    range_ends: &[i64],
) {
    let file = std::fs::File::open(filename).unwrap_or_else(|e| {
        pgrx::error!("pg_deltax: cannot open file '{}': {}", filename, e);
    });
    let mut reader = BufReader::with_capacity(8 * 1024 * 1024, file);
    let mut line_reader = CopyLineReader::new();
    let mut buf: Vec<u8> = Vec::with_capacity(16 * 1024 * 1024);

    let mut total_rows: i64 = 0;
    let mut last_part_idx: Option<usize> = None;
    let mut parse_time_us: u64 = 0;
    let copy_start = std::time::Instant::now();

    // Initial fill
    {
        let data = reader.fill_buf().unwrap_or_else(|e| {
            pgrx::error!("pg_deltax: read error: {}", e);
        });
        buf.extend_from_slice(data);
        let n = data.len();
        reader.consume(n);
    }

    // Handle HEADER
    if matches!(opts.header, HeaderMode::Skip | HeaderMode::Match(_)) {
        match line_reader.next_line(&buf, 0) {
            LineResult::Row(_, e) => {
                let eol_len = match line_reader.eol {
                    Some(crate::copyparse::Eol::CrLf) => 2,
                    _ => 1,
                };
                buf.drain(..e + eol_len);
            }
            LineResult::EndOfCopy => {
                return;
            }
            LineResult::Incomplete => {
                pgrx::error!("pg_deltax: file has no complete header line");
            }
        }
    }

    let num_columns = state.physical_column_count();
    let mut pos: usize = 0;
    let mut field_offsets: Vec<(usize, usize)> = Vec::with_capacity(num_columns);

    loop {
        let t_parse = std::time::Instant::now();
        match line_reader.next_line(&buf, pos) {
            LineResult::Row(s, e) => {
                parse_time_us += t_parse.elapsed().as_micros() as u64;

                let line_start = s;
                let line_end = e;
                split_field_offsets(
                    &buf[line_start..line_end],
                    opts.delimiter,
                    &mut field_offsets,
                );

                if field_offsets.len() != num_columns {
                    pgrx::error!(
                        "pg_deltax: line {}: expected {} fields, got {}",
                        line_reader.line_number,
                        num_columns,
                        field_offsets.len()
                    );
                }

                let (ts, te) = field_offsets[state.time_col_index];
                let time_raw = &buf[line_start + ts..line_start + te];
                if time_raw == opts.null_string.as_slice() {
                    pgrx::error!(
                        "pg_deltax: time column value is NULL at line {}, cannot route to partition",
                        line_reader.line_number
                    );
                }
                let time_str = if memchr::memchr(b'\\', time_raw).is_none() {
                    std::str::from_utf8(time_raw).unwrap_or_else(|_| {
                        pgrx::error!(
                            "pg_deltax: invalid UTF-8 in time column at line {}",
                            line_reader.line_number
                        );
                    })
                } else {
                    pgrx::error!(
                        "pg_deltax: unexpected escape in time column at line {}",
                        line_reader.line_number
                    );
                };
                let time_usec = crate::timeparse::parse_timestamp_to_usec(time_str);

                let part_idx = match find_partition(range_starts, range_ends, time_usec) {
                    Some(idx) => idx,
                    None => {
                        pgrx::error!(
                            "pg_deltax: row at line {} with timestamp {} does not fit any partition",
                            line_reader.line_number,
                            time_usec
                        );
                    }
                };

                if partitions[part_idx].is_compressed {
                    pgrx::error!(
                        "pg_deltax: partition '{}' is already compressed. Decompress it first to load new data.",
                        partitions[part_idx].table_name
                    );
                }

                if let Some(prev_idx) = last_part_idx
                    .filter(|&idx| idx != part_idx && !part_buffers[idx].blob_buffer.is_empty())
                {
                    flush_partition_blobs(&mut part_buffers[prev_idx], &state.columns);
                }
                last_part_idx = Some(part_idx);

                let pbuf = &mut part_buffers[part_idx];
                for (i, kind) in state.kinds.iter().take(num_columns).enumerate() {
                    let (fs, fe) = field_offsets[i];
                    let raw_field = &buf[line_start + fs..line_start + fe];
                    if let Err(e) = parse_raw_field_and_append(
                        raw_field,
                        &opts.null_string,
                        *kind,
                        &mut pbuf.typed_cols[i],
                        i,
                        line_reader.line_number,
                    ) {
                        pgrx::error!(
                            "pg_deltax: parse error at line {}, column {} ('{}'): {}",
                            e.line,
                            e.column,
                            state.columns[i].name,
                            e.message
                        );
                    }
                }
                // Apply JSON-path extraction for any source column with specs.
                for (i, targets) in state.extract_targets.iter().enumerate() {
                    let Some(targets) = targets else { continue };
                    let (fs, fe) = field_offsets[i];
                    let raw_field = &buf[line_start + fs..line_start + fe];
                    crate::compress::extract_from_raw_field(
                        raw_field,
                        &opts.null_string,
                        targets,
                        &mut pbuf.typed_cols,
                    );
                }
                pbuf.row_count += 1;
                total_rows += 1;

                if pbuf.row_count >= state.segment_size {
                    flush_segment(pbuf, state);
                }

                let eol_len = match line_reader.eol {
                    Some(crate::copyparse::Eol::CrLf) => 2,
                    _ => 1,
                };
                pos = e + eol_len;
            }
            LineResult::EndOfCopy => {
                break;
            }
            LineResult::Incomplete => {
                parse_time_us += t_parse.elapsed().as_micros() as u64;

                if pos > 0 {
                    buf.drain(..pos);
                    pos = 0;
                }

                let data = reader.fill_buf().unwrap_or_else(|e| {
                    pgrx::error!("pg_deltax: read error: {}", e);
                });
                if data.is_empty() {
                    if !buf.is_empty() {
                        let line = &buf[..];
                        let raw_fields = split_fields(line, opts.delimiter);
                        if raw_fields.len() == num_columns {
                            line_reader.line_number += 1;
                            handle_trailing_line(
                                &raw_fields,
                                &opts,
                                state,
                                part_buffers,
                                partitions,
                                range_starts,
                                range_ends,
                                &mut last_part_idx,
                                &mut total_rows,
                                line_reader.line_number,
                            );
                        }
                    }
                    break;
                }
                buf.extend_from_slice(data);
                let n = data.len();
                reader.consume(n);
            }
        }
    }

    let copy_elapsed = copy_start.elapsed();
    pgrx::notice!(
        "pg_deltax: COPY (Rust parser) done: {} rows in {:.1}s, parse={:.1}s ({:.0}%)",
        total_rows,
        copy_elapsed.as_secs_f64(),
        parse_time_us as f64 / 1e6,
        if copy_elapsed.as_secs_f64() > 0.0 {
            (parse_time_us as f64 / 1e6) / copy_elapsed.as_secs_f64() * 100.0
        } else {
            0.0
        }
    );
}

/// Stdin/program COPY path: use PG's BeginCopyFrom for protocol handling,
/// but NextCopyFromRawFields for line/field parsing (skipping PG's InputFunctionCall),
/// then Rust type conversion via `parse_and_append`.
///
/// When `is_csv` is true, the stripped `FORMAT deltax_compress_csv` option is
/// replaced with `FORMAT csv` so PG's CSV parser handles quoting / escaping
/// (used for files with embedded commas, e.g. jsonb columns).
#[allow(clippy::too_many_arguments)]
fn handle_copy_from_legacy(
    cs: &pg_sys::CopyStmt,
    format_idx: i32,
    is_csv: bool,
    state: &BackfillState,
    part_buffers: &mut [PartitionBuffer],
    partitions: &[crate::catalog::PartitionInfo],
    range_starts: &[i64],
    range_ends: &[i64],
) {
    let final_options = unsafe {
        if is_csv {
            replace_format_option(cs.options, format_idx, "csv")
        } else {
            // TEXT default (tab-separated with backslash escapes)
            strip_format_option(cs.options, format_idx)
        }
    };

    let rel_oid = unsafe {
        pg_sys::RangeVarGetRelidExtended(
            cs.relation,
            pg_sys::AccessShareLock as pg_sys::LOCKMODE,
            0,
            None,
            std::ptr::null_mut(),
        )
    };

    let rel = unsafe { pg_sys::table_open(rel_oid, pg_sys::AccessShareLock as pg_sys::LOCKMODE) };
    let pstate = unsafe { pg_sys::make_parsestate(std::ptr::null_mut()) };

    let cstate = unsafe {
        pg_sys::BeginCopyFrom(
            pstate,
            rel,
            cs.whereClause,
            cs.filename,
            cs.is_program,
            None, // data_source_cb
            cs.attlist,
            final_options,
        )
    };

    let num_columns = state.physical_column_count();

    let mut total_rows: i64 = 0;
    let mut last_part_idx: Option<usize> = None;
    let mut parse_time_us: u64 = 0;
    let copy_start = std::time::Instant::now();

    // NextCopyFromRawFields returns raw char** fields — PG handles the COPY
    // protocol and line/field splitting, but skips InputFunctionCall.
    // We do type conversion in Rust via parse_and_append.
    let mut raw_fields: *mut *mut std::ffi::c_char = std::ptr::null_mut();
    let mut nfields: std::ffi::c_int = 0;
    let mut line_number: u64 = 0;

    loop {
        let t_parse = std::time::Instant::now();
        let has_row =
            unsafe { pg_sys::NextCopyFromRawFields(cstate, &mut raw_fields, &mut nfields) };
        parse_time_us += t_parse.elapsed().as_micros() as u64;

        if !has_row {
            break;
        }

        line_number += 1;

        if nfields as usize != num_columns {
            pgrx::error!(
                "pg_deltax: line {}: expected {} fields, got {}",
                line_number,
                num_columns,
                nfields
            );
        }

        // Convert raw C strings to Option<&str> (NULL fields have null pointer)
        // and extract the time column value for partition routing.
        let time_str = unsafe {
            let ptr = *raw_fields.add(state.time_col_index);
            if ptr.is_null() {
                pgrx::error!(
                    "pg_deltax: time column value is NULL at line {}, cannot route to partition",
                    line_number
                );
            }
            CStr::from_ptr(ptr).to_str().unwrap_or_else(|_| {
                pgrx::error!(
                    "pg_deltax: invalid UTF-8 in time column at line {}",
                    line_number
                );
            })
        };
        let time_usec = crate::timeparse::parse_timestamp_to_usec(time_str);

        let part_idx = match find_partition(range_starts, range_ends, time_usec) {
            Some(idx) => idx,
            None => {
                pgrx::error!(
                    "pg_deltax: row at line {} with timestamp {} does not fit any partition",
                    line_number,
                    time_usec
                );
            }
        };

        if partitions[part_idx].is_compressed {
            pgrx::error!(
                "pg_deltax: partition '{}' is already compressed. Decompress it first to load new data.",
                partitions[part_idx].table_name
            );
        }

        if let Some(prev_idx) = last_part_idx
            .filter(|&idx| idx != part_idx && !part_buffers[idx].blob_buffer.is_empty())
        {
            flush_partition_blobs(&mut part_buffers[prev_idx], &state.columns);
        }
        last_part_idx = Some(part_idx);

        // Append each field using Rust type conversion
        let pbuf = &mut part_buffers[part_idx];
        for i in 0..num_columns {
            let field_str: Option<&str> = unsafe {
                let ptr = *raw_fields.add(i);
                if ptr.is_null() {
                    None
                } else {
                    Some(CStr::from_ptr(ptr).to_str().unwrap_or_else(|_| {
                        pgrx::error!(
                            "pg_deltax: invalid UTF-8 in column {} at line {}",
                            i,
                            line_number
                        );
                    }))
                }
            };

            if let Err(e) = parse_and_append(
                field_str,
                state.kinds[i],
                &mut pbuf.typed_cols[i],
                i,
                line_number,
            ) {
                pgrx::error!(
                    "pg_deltax: parse error at line {}, column {} ('{}'): {}",
                    e.line,
                    e.column,
                    state.columns[i].name,
                    e.message
                );
            }
            // Apply JSON-path extraction for this source column if it has specs.
            if let Some(Some(targets)) = state.extract_targets.get(i) {
                let field_str_for_extract: Option<&str> = unsafe {
                    let ptr = *raw_fields.add(i);
                    if ptr.is_null() {
                        None
                    } else {
                        CStr::from_ptr(ptr).to_str().ok()
                    }
                };
                crate::compress::extract_from_str_field(
                    field_str_for_extract,
                    targets,
                    &mut pbuf.typed_cols,
                );
            }
        }
        pbuf.row_count += 1;
        total_rows += 1;

        if pbuf.row_count >= state.segment_size {
            flush_segment(pbuf, state);
        }
    }

    let copy_elapsed = copy_start.elapsed();
    pgrx::notice!(
        "pg_deltax: COPY (Rust types, PG protocol) done: {} rows in {:.1}s, parse={:.1}s ({:.0}%)",
        total_rows,
        copy_elapsed.as_secs_f64(),
        parse_time_us as f64 / 1e6,
        if copy_elapsed.as_secs_f64() > 0.0 {
            (parse_time_us as f64 / 1e6) / copy_elapsed.as_secs_f64() * 100.0
        } else {
            0.0
        }
    );

    unsafe { pg_sys::EndCopyFrom(cstate) };
    unsafe { pg_sys::free_parsestate(pstate) };
    unsafe { pg_sys::table_close(rel, pg_sys::AccessShareLock as pg_sys::LOCKMODE) };
}

/// Build a per-row length sidecar blob for a text column; None for non-text types.
fn build_text_length_blob(col: &TypedColumn, data_type: &str) -> Option<Vec<u8>> {
    if !is_text_data_type(&data_type.to_lowercase()) {
        return None;
    }
    match col {
        TypedColumn::Text(values) => Some(compress_text_lengths(values)),
        _ => None,
    }
}

/// Compress one column and gather its per-segment stats. Pure Rust (no PG
/// calls), safe to invoke from worker threads.
fn compress_one_col(
    col_idx: u16,
    col_i: usize,
    typed_cols: &[TypedColumn],
    columns: &[ColumnMeta],
) -> ColResult {
    let col = &columns[col_i];
    let compressed = compress_typed_column(&typed_cols[col_i], &col.data_type);
    let (min_val, max_val) = if supports_minmax(&col.data_type) {
        compute_typed_minmax(&typed_cols[col_i], &col.data_type)
    } else {
        (None, None)
    };
    let (sum_val, nonnull_count, nonzero_count) = if supports_sum(&col.data_type) {
        compute_typed_sum(&typed_cols[col_i])
    } else {
        (None, 0, 0)
    };
    let text_length_blob = build_text_length_blob(&typed_cols[col_i], &col.data_type);
    ColResult {
        col_idx,
        col_i,
        compressed,
        min_val,
        max_val,
        sum_val,
        nonnull_count,
        nonzero_count,
        text_length_blob,
    }
}

/// Compress every non-segment-by column in parallel when worth it; fall
/// back to single-threaded when `n_workers <= 1` or there's a single column.
fn parallel_compress_cols(
    non_segby: &[(u16, usize)],
    typed_cols: &[TypedColumn],
    columns: &[ColumnMeta],
    n_workers: usize,
) -> Vec<ColResult> {
    if n_workers > 1 && non_segby.len() > 1 {
        let chunk_size = non_segby.len().div_ceil(n_workers);
        std::thread::scope(|s| {
            non_segby
                .chunks(chunk_size)
                .map(|chunk| {
                    s.spawn(move || {
                        chunk
                            .iter()
                            .map(|&(col_idx, col_i)| {
                                compress_one_col(col_idx, col_i, typed_cols, columns)
                            })
                            .collect::<Vec<_>>()
                    })
                })
                .collect::<Vec<_>>()
                .into_iter()
                .flat_map(|h| h.join().unwrap())
                .collect()
        })
    } else {
        non_segby
            .iter()
            .map(|&(col_idx, col_i)| compress_one_col(col_idx, col_i, typed_cols, columns))
            .collect()
    }
}

/// Per-column compression result produced by worker threads.
struct ColResult {
    col_idx: u16,
    col_i: usize,
    compressed: Vec<u8>,
    min_val: Option<String>,
    max_val: Option<String>,
    sum_val: Option<String>,
    nonnull_count: i64,
    nonzero_count: i64,
    /// Per-row length sidecar, only populated for text-family columns.
    text_length_blob: Option<Vec<u8>>,
}

/// A fully compressed segment ready for heap_insert on the main thread.
/// Produced by compress workers, consumed by the main thread's flush loop.
struct CompressedSegment {
    partition_idx: usize,
    seg_id: i32,
    row_count: usize,
    blobs: Vec<(u16, Vec<u8>)>,             // (col_idx, compressed_data)
    bloom_entries: Vec<(u16, u8, Vec<u8>)>, // (col_idx, num_hashes, bytes); empty if blooms disabled
    /// Per-text-column length sidecars (col_idx, length_blob).
    text_length_blobs: Vec<(u16, Vec<u8>)>,
    /// Per-text-column distinct-value sets for low-cardinality (≤32) columns.
    /// Encoded into bitmaps in `finalize_partition` once the partition-level
    /// value list is finalized.
    valbitmap_value_sets: Vec<(u16, Vec<String>)>,
    meta_values_csv: String, // pre-formatted VALUES clause for thin meta
    colstats_rows_csv: Vec<String>, // pre-formatted VALUES tuples, one per non-segment-by column
    total_compressed_size: i64,
}

/// Sort, compress, and prepare metadata for a segment (pure Rust, no PG calls).
/// Returns a CompressedSegment ready for heap_insert by the main thread.
#[allow(clippy::too_many_arguments)]
fn compress_segment(
    mut typed_cols: Vec<TypedColumn>,
    row_count: usize,
    seg_id: i32,
    partition_idx: usize,
    columns: &[ColumnMeta],
    order_col_indices: &[usize],
    bloom_enabled: bool,
    n_workers: usize,
) -> CompressedSegment {
    // Sort
    sort_typed_columns(&mut typed_cols, order_col_indices, row_count);

    // Ndistinct
    let (ndistinct, _hll_sketches) = compute_segment_ndistinct(&typed_cols, columns);

    // Segment_by values
    let seg_values: Vec<Option<String>> = columns
        .iter()
        .enumerate()
        .filter(|(_, c)| c.is_segment_by)
        .map(|(i, _)| {
            if let TypedColumn::Text(v) = &typed_cols[i] {
                if v.is_empty() { None } else { v[0].clone() }
            } else {
                None
            }
        })
        .collect();

    // Build non-segment-by column list
    let non_segby: Vec<(u16, usize)> = {
        let mut col_idx: u16 = 0;
        let mut result = Vec::new();
        for (i, col) in columns.iter().enumerate() {
            if col.is_segment_by {
                continue;
            }
            result.push((col_idx, i));
            col_idx += 1;
        }
        result
    };
    let col_results = parallel_compress_cols(&non_segby, &typed_cols, columns, n_workers);

    // Build blobs and meta
    let mut total_size: i64 = 0;
    let mut blobs: Vec<(u16, Vec<u8>)> = Vec::new();
    let mut text_length_blobs: Vec<(u16, Vec<u8>)> = Vec::new();
    let mut col_minmax: std::collections::HashMap<usize, (Option<String>, Option<String>)> =
        std::collections::HashMap::new();
    let mut col_sums: std::collections::HashMap<usize, (Option<String>, i64, i64)> =
        std::collections::HashMap::new();

    for cr in &col_results {
        total_size += cr.compressed.len() as i64;
        if supports_minmax(&columns[cr.col_i].data_type) {
            col_minmax.insert(cr.col_i, (cr.min_val.clone(), cr.max_val.clone()));
        }
        if supports_sum(&columns[cr.col_i].data_type) {
            col_sums.insert(
                cr.col_i,
                (cr.sum_val.clone(), cr.nonnull_count, cr.nonzero_count),
            );
        }
    }
    for cr in col_results {
        if let Some(length_blob) = cr.text_length_blob {
            text_length_blobs.push((cr.col_idx, length_blob));
        }
        blobs.push((cr.col_idx, cr.compressed));
    }

    // Build VALUES clause for thin meta: segment_id, segment_by, time min/max, row_count
    let mut meta_vals = Vec::new();
    meta_vals.push(seg_id.to_string());

    let mut seg_idx = 0;
    for col in columns {
        if col.is_segment_by && seg_idx < seg_values.len() {
            match &seg_values[seg_idx] {
                Some(v) => meta_vals.push(format!("'{}'", v.replace('\'', "''"))),
                None => meta_vals.push("NULL".to_string()),
            }
            seg_idx += 1;
        }
    }

    // Time column min/max only
    for (i, col) in columns.iter().enumerate() {
        if col.is_time_column && !col.is_segment_by && supports_minmax(&col.data_type) {
            match col_minmax.get(&i) {
                Some((Some(min_val), Some(max_val))) => {
                    meta_vals.push(format_minmax_for_insert(min_val, &col.data_type));
                    meta_vals.push(format_minmax_for_insert(max_val, &col.data_type));
                }
                _ => {
                    meta_vals.push("NULL".to_string());
                    meta_vals.push("NULL".to_string());
                }
            }
        }
    }

    meta_vals.push((row_count as u32).to_string());

    // Build per-column colstats rows (normalized: one row per non-segment-by column)
    let mut colstats_rows_csv: Vec<String> = Vec::new();
    let mut nd_idx = 0;
    for (i, col) in columns.iter().enumerate() {
        if col.is_segment_by {
            continue;
        }
        let col_idx = colstats_rows_csv.len() as i16;
        let (min_enc, max_enc) = compute_minmax_encoded_i64(&typed_cols[i], &col.data_type);
        let min_str = min_enc.map_or("NULL".to_string(), |v| v.to_string());
        let max_str = max_enc.map_or("NULL".to_string(), |v| v.to_string());

        let (sum_str, nonnull, nonzero) = if supports_sum(&col.data_type) {
            match col_sums.get(&i) {
                Some((Some(sum_val), nn, nz)) => (sum_val.clone(), *nn, *nz),
                _ => ("NULL".to_string(), 0, 0),
            }
        } else {
            ("NULL".to_string(), 0, 0)
        };

        let nd = if nd_idx < ndistinct.len() {
            ndistinct[nd_idx]
        } else {
            0
        };
        nd_idx += 1;

        colstats_rows_csv.push(format!(
            "({}, {}, {}, {}, {}, {}, {}, {})",
            col_idx, seg_id, min_str, max_str, sum_str, nonnull, nonzero, nd
        ));
    }

    // Bloom filters
    let bloom_entries = if bloom_enabled {
        compute_segment_blooms(&typed_cols, columns, &ndistinct)
    } else {
        Vec::new()
    };

    // Per-segment value sets for low-cardinality text columns.
    let valbitmap_value_sets =
        crate::compress::compute_segment_valbitmap_values(&typed_cols, columns);

    CompressedSegment {
        partition_idx,
        seg_id,
        row_count,
        blobs,
        bloom_entries,
        text_length_blobs,
        valbitmap_value_sets,
        meta_values_csv: meta_vals.join(", "),
        colstats_rows_csv,
        total_compressed_size: total_size,
    }
}

/// Flush a full segment from a partition buffer, using parallel compression.
fn flush_segment(buf: &mut PartitionBuffer, state: &BackfillState) {
    let t_start = std::time::Instant::now();

    // Ensure companion tables exist (created together on first segment).
    // Blobs and blooms tables are created WITHOUT primary keys for fast heap_insert.
    // PKs are added in finalize_partition after all data is loaded.
    if !buf.meta_table_created {
        let ddl = build_companion_ddl(&buf.partition_table, &state.columns);
        spi_exec(&ddl.meta_ddl);
        spi_exec(&ddl.colstats_ddl);
        create_blobs_table(&ddl.blobs_fqn);
        if crate::BLOOM_FILTERS.get() {
            create_blooms_table(&ddl.blooms_fqn);
        }
        buf.meta_table_created = true;
        buf.blobs_table_created = true;
    }

    // Sort by order_by columns
    let t_sort_start = std::time::Instant::now();
    sort_typed_columns(&mut buf.typed_cols, &state.order_col_indices, buf.row_count);
    let sort_ms = t_sort_start.elapsed().as_millis();

    // Compute ndistinct
    let (ndistinct, _hll_sketches) = compute_segment_ndistinct(&buf.typed_cols, &state.columns);

    // Segment_by values from the buffered data (extract from first row if present)
    let seg_values: Vec<Option<String>> = state
        .columns
        .iter()
        .enumerate()
        .filter(|(_, c)| c.is_segment_by)
        .map(|(i, _)| {
            if let TypedColumn::Text(v) = &buf.typed_cols[i] {
                if v.is_empty() { None } else { v[0].clone() }
            } else {
                None
            }
        })
        .collect();

    let seg_id = buf.next_segment_id;
    buf.next_segment_id += 1;

    // Build list of non-segment-by column indices for parallel compression
    let non_segby: Vec<(u16, usize)> = {
        let mut col_idx: u16 = 0;
        let mut result = Vec::new();
        for (i, col) in state.columns.iter().enumerate() {
            if col.is_segment_by {
                continue;
            }
            result.push((col_idx, i));
            col_idx += 1;
        }
        result
    };

    // Parallel compression: distribute columns across workers
    let t_compress_start = std::time::Instant::now();
    let n_workers = crate::get_parallel_workers();
    let col_results =
        parallel_compress_cols(&non_segby, &buf.typed_cols, &state.columns, n_workers);
    let compress_ms = t_compress_start.elapsed().as_millis();

    // Build meta INSERT SQL on main thread
    let t_meta_start = std::time::Instant::now();
    let mut total_size: i64 = 0;
    let mut blobs: Vec<(u16, Vec<u8>)> = Vec::new();
    let mut length_blobs: Vec<(u16, Vec<u8>)> = Vec::new();

    // Index col_results by col_i for lookup
    let mut col_minmax: std::collections::HashMap<usize, (Option<String>, Option<String>)> =
        std::collections::HashMap::new();
    let mut col_sums: std::collections::HashMap<usize, (Option<String>, i64, i64)> =
        std::collections::HashMap::new();

    for cr in &col_results {
        total_size += cr.compressed.len() as i64;
        if supports_minmax(&state.columns[cr.col_i].data_type) {
            col_minmax.insert(cr.col_i, (cr.min_val.clone(), cr.max_val.clone()));
        }
        if supports_sum(&state.columns[cr.col_i].data_type) {
            col_sums.insert(
                cr.col_i,
                (cr.sum_val.clone(), cr.nonnull_count, cr.nonzero_count),
            );
        }
    }
    for cr in col_results {
        if let Some(length_blob) = cr.text_length_blob {
            length_blobs.push((cr.col_idx, length_blob));
        }
        blobs.push((cr.col_idx, cr.compressed));
    }

    // Build VALUES clause for thin meta row: segment_id, segment_by, time min/max, row_count
    let mut meta_vals = Vec::new();
    meta_vals.push(seg_id.to_string());

    // Segment-by columns
    let mut seg_idx = 0;
    for col in &state.columns {
        if col.is_segment_by && seg_idx < seg_values.len() {
            match &seg_values[seg_idx] {
                Some(v) => meta_vals.push(format!("'{}'", v.replace('\'', "''"))),
                None => meta_vals.push("NULL".to_string()),
            }
            seg_idx += 1;
        }
    }

    // Time column min/max only
    for (i, col) in state.columns.iter().enumerate() {
        if col.is_time_column && !col.is_segment_by && supports_minmax(&col.data_type) {
            match col_minmax.get(&i) {
                Some((Some(min_val), Some(max_val))) => {
                    meta_vals.push(format_minmax_for_insert(min_val, &col.data_type));
                    meta_vals.push(format_minmax_for_insert(max_val, &col.data_type));
                }
                _ => {
                    meta_vals.push("NULL".to_string());
                    meta_vals.push("NULL".to_string());
                }
            }
        }
    }

    meta_vals.push((buf.row_count as u32).to_string());

    // Build per-column colstats rows (normalized: one row per non-segment-by column)
    let mut colstats_rows_csv: Vec<String> = Vec::new();
    let mut nd_idx = 0;
    for (i, col) in state.columns.iter().enumerate() {
        if col.is_segment_by {
            continue;
        }
        let col_idx = colstats_rows_csv.len() as i16;
        let (min_enc, max_enc) = compute_minmax_encoded_i64(&buf.typed_cols[i], &col.data_type);
        let min_str = min_enc.map_or("NULL".to_string(), |v| v.to_string());
        let max_str = max_enc.map_or("NULL".to_string(), |v| v.to_string());

        let (sum_str, nonnull, nonzero) = if supports_sum(&col.data_type) {
            match col_sums.get(&i) {
                Some((Some(sum_val), nn, nz)) => (sum_val.clone(), *nn, *nz),
                _ => ("NULL".to_string(), 0, 0),
            }
        } else {
            ("NULL".to_string(), 0, 0)
        };

        let nd = if nd_idx < ndistinct.len() {
            ndistinct[nd_idx]
        } else {
            0
        };
        nd_idx += 1;

        colstats_rows_csv.push(format!(
            "({}, {}, {}, {}, {}, {}, {}, {})",
            col_idx, seg_id, min_str, max_str, sum_str, nonnull, nonzero, nd
        ));
    }

    // Bloom filters
    let bloom_entries = if crate::BLOOM_FILTERS.get() {
        compute_segment_blooms(&buf.typed_cols, &state.columns, &ndistinct)
    } else {
        Vec::new()
    };

    // Cache table names and column lists (same for every segment of this partition)
    buf.cache_companion_fqns(&state.columns);

    // Buffer the VALUES rows
    buf.meta_insert_rows
        .push(format!("({})", meta_vals.join(", ")));
    for row in colstats_rows_csv {
        buf.colstats_insert_rows.push(row);
    }

    // Flush meta batch if full
    if buf.meta_insert_rows.len() >= META_BATCH_SIZE {
        flush_meta_buffer(buf);
    }
    let meta_ms = t_meta_start.elapsed().as_millis();

    buf.total_compressed_size += total_size;
    buf.total_rows += buf.row_count as i64;

    // Buffer blobs for column-major flush
    for (col_idx, blob) in blobs {
        buf.blob_buffer_size += blob.len();
        buf.blob_buffer.push((col_idx, seg_id, blob));
    }
    for (col_idx, num_hashes, bytes) in bloom_entries {
        buf.bloom_buffer.push((col_idx, seg_id, num_hashes, bytes));
    }
    for (col_idx, length_blob) in length_blobs {
        buf.text_length_buffer.push((col_idx, seg_id, length_blob));
    }
    let vb_values =
        crate::compress::compute_segment_valbitmap_values(&buf.typed_cols, &state.columns);
    for (col_idx, vals) in vb_values {
        buf.valbitmap_value_buffer.push((col_idx, seg_id, vals));
    }

    // Flush blobs immediately if buffer exceeds threshold to bound memory usage.
    let t_blob_flush_start = std::time::Instant::now();
    let did_flush_blobs = buf.blob_buffer_size >= BLOB_BUFFER_THRESHOLD;
    if did_flush_blobs {
        flush_partition_blobs(buf, &state.columns);
    }
    let blob_flush_ms = t_blob_flush_start.elapsed().as_millis();

    // Log timing every 100 segments for profiling
    let total_ms = t_start.elapsed().as_millis();
    if buf.next_segment_id % 100 == 0 || did_flush_blobs {
        pgrx::notice!(
            "pg_deltax: segment timing: sort={}ms compress={}ms meta={}ms blob_flush={}ms total={}ms (workers={}, {} rows)",
            sort_ms,
            compress_ms,
            meta_ms,
            blob_flush_ms,
            total_ms,
            n_workers,
            buf.row_count
        );
    }

    // Reset for next segment
    buf.typed_cols = init_typed_columns(&state.columns, &state.kinds);
    buf.row_count = 0;
}

/// Flush buffered meta rows as multi-row INSERTs.
/// Note: colstats rows are NOT flushed here — they are accumulated until
/// finalize time and flushed sorted by (_col_idx, _segment_id) for
/// column-major heap layout.
fn flush_meta_buffer(buf: &mut PartitionBuffer) {
    if !buf.meta_insert_rows.is_empty() {
        let meta_fqn = buf.meta_fqn.as_ref().expect("meta_fqn not set");
        let cols = buf
            .meta_insert_cols
            .as_ref()
            .expect("meta_insert_cols not set");
        let insert_sql = format!(
            "INSERT INTO {} ({}) VALUES {}",
            meta_fqn,
            cols,
            buf.meta_insert_rows.join(", ")
        );
        spi_exec(&insert_sql);
        buf.meta_insert_rows.clear();
    }
}

/// Flush all buffered colstats rows sorted by (_col_idx, _segment_id) so that
/// the heap is naturally clustered for index scans by _col_idx.
fn flush_colstats_buffer(buf: &mut PartitionBuffer) {
    if buf.colstats_insert_rows.is_empty() {
        return;
    }
    let colstats_fqn = buf.colstats_fqn.as_ref().expect("colstats_fqn not set");

    // Each row string is "(col_idx, segment_id, ...)". Sort by parsing the leading integers.
    buf.colstats_insert_rows.sort_by(|a, b| {
        fn parse_key(s: &str) -> (i16, i32) {
            // Strip leading '(' and parse first two comma-separated integers
            let inner = s.trim_start_matches('(');
            let mut parts = inner.splitn(3, ',');
            let col_idx: i16 = parts.next().unwrap_or("0").trim().parse().unwrap_or(0);
            let seg_id: i32 = parts.next().unwrap_or("0").trim().parse().unwrap_or(0);
            (col_idx, seg_id)
        }
        parse_key(a).cmp(&parse_key(b))
    });

    // Flush in batches
    let batch_size = 100;
    for chunk in buf.colstats_insert_rows.chunks(batch_size) {
        let insert_sql = format!(
            "INSERT INTO {} (_col_idx, _segment_id, _min, _max, _sum, _nonnull_count, _nonzero_count, _ndistinct) VALUES {}",
            colstats_fqn,
            chunk.join(", ")
        );
        spi_exec(&insert_sql);
    }
    buf.colstats_insert_rows.clear();
}

/// Flush a partition's blob and bloom buffers to the companion tables.
/// Called when the COPY loop moves to a different partition (for time-sorted data),
/// or at end-of-COPY for remaining buffers. This keeps peak memory bounded to
/// one partition's worth of compressed blobs at a time.
fn flush_partition_blobs(buf: &mut PartitionBuffer, columns: &[ColumnMeta]) {
    if buf.blob_buffer.is_empty()
        && buf.bloom_buffer.is_empty()
        && buf.text_length_buffer.is_empty()
    {
        return;
    }

    // Cache companion table FQNs on first call
    if buf.blobs_fqn_cached.is_none() {
        let ddl = build_companion_ddl(&buf.partition_table, columns);
        buf.blobs_fqn_cached = Some(ddl.blobs_fqn);
        buf.blooms_fqn_cached = Some(ddl.blooms_fqn);
        buf.text_lengths_fqn_cached = Some(ddl.text_lengths_fqn);
    }
    let blobs_fqn = buf.blobs_fqn_cached.as_ref().unwrap();
    let blooms_fqn = buf.blooms_fqn_cached.as_ref().unwrap();
    let text_lengths_fqn = buf.text_lengths_fqn_cached.as_ref().unwrap();

    // Create tables without PK for fast heap_insert (PK added in finalize_partition)
    if !buf.blobs_table_created {
        create_blobs_table(blobs_fqn);
        if crate::BLOOM_FILTERS.get() {
            create_blooms_table(blooms_fqn);
        }
        buf.blobs_table_created = true;
    }
    if !buf.text_lengths_table_created && !buf.text_length_buffer.is_empty() {
        create_text_lengths_table(text_lengths_fqn);
        buf.text_lengths_table_created = true;
    }

    // Use direct heap_insert bypassing SPI entirely. This avoids per-INSERT
    // executor overhead, plan caching, and catalog cache bloat. BulkInsertState
    // uses a ring buffer to avoid polluting shared_buffers. A per-row temp
    // context (created inside `bulk_heap_insert`) bounds memory: heap_insert
    // calls toast_insert_or_update which palloc's compressed copies in
    // CurrentMemoryContext; without resetting between rows these accumulate
    // for the entire transaction (~30 GB on ClickBench).

    // Sort blobs column-major (col_idx, segment_id) for sequential TOAST I/O on read.
    if !buf.blob_buffer.is_empty() {
        buf.blob_buffer
            .sort_by_key(|&(col_idx, seg_id, _)| (col_idx, seg_id));
        let blobs_oid = *buf
            .blobs_oid_cached
            .get_or_insert_with(|| resolve_relation_oid(blobs_fqn));
        let drained: Vec<_> = buf.blob_buffer.drain(..).collect();
        unsafe {
            bulk_heap_insert(
                blobs_oid,
                c"direct_backfill_insert",
                drained,
                |(col_idx, seg_id, blob)| {
                    vec![
                        pg_sys::Datum::from(*col_idx as i16),
                        pg_sys::Datum::from(*seg_id),
                        bytea_to_datum(blob),
                    ]
                },
            );
        }
    }

    if !buf.bloom_buffer.is_empty() {
        buf.bloom_buffer
            .sort_by_key(|&(col_idx, seg_id, _, _)| (col_idx, seg_id));
        let blooms_oid = *buf
            .blooms_oid_cached
            .get_or_insert_with(|| resolve_relation_oid(blooms_fqn));
        let drained: Vec<_> = buf.bloom_buffer.drain(..).collect();
        unsafe {
            bulk_heap_insert(
                blooms_oid,
                c"direct_backfill_bloom_insert",
                drained,
                |(col_idx, seg_id, num_hashes, bytes)| {
                    vec![
                        pg_sys::Datum::from(*col_idx as i16),
                        pg_sys::Datum::from(*seg_id),
                        pg_sys::Datum::from(*num_hashes as i16),
                        bytea_to_datum(bytes),
                    ]
                },
            );
        }
    }

    if !buf.text_length_buffer.is_empty() {
        buf.text_length_buffer
            .sort_by_key(|&(col_idx, seg_id, _)| (col_idx, seg_id));
        let text_lengths_oid = *buf
            .text_lengths_oid_cached
            .get_or_insert_with(|| resolve_relation_oid(text_lengths_fqn));
        let drained: Vec<_> = buf.text_length_buffer.drain(..).collect();
        unsafe {
            bulk_heap_insert(
                text_lengths_oid,
                c"direct_backfill_text_lengths_insert",
                drained,
                |(col_idx, seg_id, length_blob)| {
                    vec![
                        pg_sys::Datum::from(*col_idx as i16),
                        pg_sys::Datum::from(*seg_id),
                        bytea_to_datum(length_blob),
                    ]
                },
            );
        }
    }

    pgrx::notice!(
        "pg_deltax: flushed {} MB of blobs for partition '{}' ({} rows total)",
        buf.blob_buffer_size / (1024 * 1024),
        buf.partition_table,
        buf.total_rows
    );
    buf.blob_buffer_size = 0;
    buf.blobs_flushed = true;
}

/// Write a pre-compressed segment into the partition buffer and flush to PG when threshold is reached.
/// This is the I/O-only counterpart to `compress_segment` (which does all the CPU work).
fn write_compressed_segment(
    cs: CompressedSegment,
    buf: &mut PartitionBuffer,
    state: &BackfillState,
) {
    // Ensure companion tables exist
    if !buf.meta_table_created {
        let ddl = build_companion_ddl(&buf.partition_table, &state.columns);
        spi_exec(&ddl.meta_ddl);
        spi_exec(&ddl.colstats_ddl);
        create_blobs_table(&ddl.blobs_fqn);
        if crate::BLOOM_FILTERS.get() {
            create_blooms_table(&ddl.blooms_fqn);
        }
        buf.meta_table_created = true;
        buf.blobs_table_created = true;
    }

    // Cache meta and colstats column lists
    buf.cache_companion_fqns(&state.columns);

    // Buffer meta and colstats rows
    buf.meta_insert_rows
        .push(format!("({})", cs.meta_values_csv));
    for row in cs.colstats_rows_csv {
        buf.colstats_insert_rows.push(row);
    }
    if buf.meta_insert_rows.len() >= META_BATCH_SIZE {
        flush_meta_buffer(buf);
    }

    buf.total_compressed_size += cs.total_compressed_size;
    buf.total_rows += cs.row_count as i64;

    // Track segment_id for next allocation
    if cs.seg_id >= buf.next_segment_id {
        buf.next_segment_id = cs.seg_id + 1;
    }

    // Buffer blobs
    for (col_idx, blob) in cs.blobs {
        buf.blob_buffer_size += blob.len();
        buf.blob_buffer.push((col_idx, cs.seg_id, blob));
    }
    for (col_idx, num_hashes, bytes) in cs.bloom_entries {
        buf.bloom_buffer
            .push((col_idx, cs.seg_id, num_hashes, bytes));
    }
    for (col_idx, length_blob) in cs.text_length_blobs {
        buf.text_length_buffer
            .push((col_idx, cs.seg_id, length_blob));
    }
    for (col_idx, vals) in cs.valbitmap_value_sets {
        buf.valbitmap_value_buffer.push((col_idx, cs.seg_id, vals));
    }

    // Flush blobs when threshold reached
    if buf.blob_buffer_size >= BLOB_BUFFER_THRESHOLD {
        flush_partition_blobs(buf, &state.columns);
    }
}

/// ANALYZE companion tables and mark partition as compressed in catalog.
fn finalize_partition(buf: &mut PartitionBuffer, columns: &[ColumnMeta]) {
    if buf.total_rows == 0 {
        return;
    }

    let ddl = build_companion_ddl(&buf.partition_table, columns);

    // Add primary keys now that all data is loaded (much faster than maintaining
    // indexes during insert — PostgreSQL builds the B-tree in a single sort pass).
    spi_exec(&format!(
        "ALTER TABLE {} ADD PRIMARY KEY (_col_idx, _segment_id)",
        ddl.blobs_fqn
    ));
    if buf.blobs_table_created && crate::BLOOM_FILTERS.get() {
        spi_exec(&format!(
            "ALTER TABLE {} ADD PRIMARY KEY (_col_idx, _segment_id)",
            ddl.blooms_fqn
        ));
    }
    if buf.text_lengths_table_created {
        spi_exec(&format!(
            "ALTER TABLE {} ADD PRIMARY KEY (_col_idx, _segment_id)",
            ddl.text_lengths_fqn
        ));
    }

    // Flush colstats sorted by (_col_idx, _segment_id) for column-major heap layout
    flush_colstats_buffer(buf);

    // Btree index on (_col_idx, _min, _max) for point-lookup segment pruning.
    // See compress::compress_partition_streaming for the rationale.
    spi_exec(&format!(
        "CREATE INDEX ON {} (_col_idx, _min, _max)",
        ddl.colstats_fqn
    ));

    // Encode + insert per-segment value-presence bitmaps. The buffer holds
    // per-segment value sets; finalize the partition-level value→bit_idx
    // map, drop columns that overflowed VALBITMAP_MAX_DISTINCT, encode each
    // segment's bitmap, bulk-insert. Returns the column→values map for the
    // catalog write below.
    let column_valmap = finalize_and_insert_valbitmap(buf, columns, &ddl);

    spi_exec(&format!("ANALYZE {}", ddl.meta_fqn));
    spi_exec(&format!("ANALYZE {}", ddl.colstats_fqn));
    spi_exec(&format!("ANALYZE {}", ddl.blobs_fqn));

    if buf.blobs_table_created && crate::BLOOM_FILTERS.get() {
        spi_exec(&format!("ANALYZE {}", ddl.blooms_fqn));
    }
    if buf.text_lengths_table_created {
        spi_exec(&format!("ANALYZE {}", ddl.text_lengths_fqn));
    }

    // Use a short-lived SPI connection for catalog update
    let partition_id = buf.partition_id;
    let total_compressed_size = buf.total_compressed_size;
    let total_rows = buf.total_rows;
    let nd_col_names: Vec<String> = columns
        .iter()
        .filter(|c| !c.is_segment_by)
        .map(|c| c.name.clone())
        .collect();
    Spi::connect_mut(|client| {
        catalog::mark_partition_compressed(
            client,
            partition_id,
            total_compressed_size,
            0, // raw_size not meaningful for direct backfill
            total_rows,
        )
        .expect("failed to update partition catalog");
        catalog::install_compressed_dml_trigger(
            client,
            &buf.partition_schema,
            &buf.partition_table,
        )
        .expect("failed to install compressed partition DML trigger");
        catalog::update_partition_column_ndistinct(
            client,
            partition_id,
            &ddl.colstats_fqn,
            &nd_col_names,
        )
        .expect("failed to update partition column_ndistinct");
        catalog::update_partition_column_valmap(client, partition_id, &column_valmap)
            .expect("failed to update partition column_valmap");
        catalog::update_partition_column_minmax(client, partition_id, &ddl.colstats_fqn, columns)
            .expect("failed to update partition column_minmax");

        // Snapshot the physical-column shape so a later ADD COLUMN on the
        // parent doesn't desync this partition's blobs. See
        // dev/docs/SCHEMA_CHANGES.md.
        let partition_row =
            catalog::get_partition_by_name(client, &buf.partition_schema, &buf.partition_table)
                .expect("failed to query partition for compressed_columns snapshot")
                .expect("partition row missing during finalize");
        let deltatable = catalog::get_deltatable_by_id(client, partition_row.deltatable_id)
            .expect("failed to query deltatable for compressed_columns snapshot")
            .expect("deltatable row missing during finalize");
        let cc_json = catalog::snapshot_compressed_columns(
            client,
            &deltatable.schema_name,
            &deltatable.table_name,
            &deltatable.segment_by,
        )
        .expect("failed to snapshot compressed_columns");
        catalog::update_partition_compressed_columns(client, partition_id, &cc_json)
            .expect("failed to update partition compressed_columns");
    });
}

/// Build partition-level value→bit_idx maps from per-segment value sets,
/// encode each segment's bitmap, bulk-insert into the valbitmap table.
/// Returns the partition-level value list keyed by user column name (for
/// the catalog write).
///
/// Mirrors `compress::finalize_and_insert_valbitmaps` but uses `spi_exec`
/// for inserts (the direct-backfill path doesn't have a long-lived
/// `SpiClient` available).
fn finalize_and_insert_valbitmap(
    buf: &mut PartitionBuffer,
    columns: &[ColumnMeta],
    ddl: &crate::compress::CompanionDdl,
) -> std::collections::HashMap<String, Vec<String>> {
    use std::collections::{BTreeSet, HashMap};

    let value_buffer = std::mem::take(&mut buf.valbitmap_value_buffer);
    if value_buffer.is_empty() {
        return HashMap::new();
    }

    // Aggregate per-col_idx union, dropping columns that overflow.
    let mut union_by_col: HashMap<u16, BTreeSet<String>> = HashMap::new();
    let mut overflow_cols: std::collections::HashSet<u16> = std::collections::HashSet::new();
    for (col_idx, _seg_id, vals) in &value_buffer {
        if overflow_cols.contains(col_idx) {
            continue;
        }
        let entry = union_by_col.entry(*col_idx).or_default();
        for v in vals {
            if entry.len() >= crate::compress::VALBITMAP_MAX_DISTINCT && !entry.contains(v) {
                overflow_cols.insert(*col_idx);
                union_by_col.remove(col_idx);
                break;
            }
            entry.insert(v.clone());
        }
    }

    if union_by_col.is_empty() {
        return HashMap::new();
    }

    // Finalize per-column sorted value list + value→bit_idx index.
    let mut finalized: HashMap<u16, (Vec<String>, HashMap<String, u8>)> = HashMap::new();
    for (col_idx, set) in union_by_col {
        let sorted: Vec<String> = set.into_iter().collect();
        let mut idx: HashMap<String, u8> = HashMap::new();
        for (i, v) in sorted.iter().enumerate() {
            idx.insert(v.clone(), i as u8);
        }
        finalized.insert(col_idx, (sorted, idx));
    }

    // Map non-segment-by col_idx → user column name for the catalog payload.
    let col_idx_to_name: HashMap<u16, String> = {
        let mut m = HashMap::new();
        let mut idx: u16 = 0;
        for col in columns {
            if col.is_segment_by {
                continue;
            }
            m.insert(idx, col.name.clone());
            idx += 1;
        }
        m
    };

    // Create the valbitmap table (without PK; we'll add it after bulk insert,
    // mirroring the blobs/blooms pattern in this file for fast heap_insert).
    spi_exec(&ddl.valbitmap_ddl);

    // Encode + bulk-insert per-segment bitmaps.
    let mut entries: Vec<(u16, i32, Vec<u8>)> = Vec::with_capacity(value_buffer.len());
    for (col_idx, seg_id, vals) in value_buffer {
        let Some((_, idx_map)) = finalized.get(&col_idx) else {
            continue;
        };
        let n_bits = idx_map.len();
        let n_bytes = n_bits.div_ceil(8);
        let mut bits: Vec<u8> = vec![0; n_bytes];
        for v in &vals {
            if let Some(&bit_idx) = idx_map.get(v) {
                bits[(bit_idx / 8) as usize] |= 1u8 << (bit_idx % 8);
            }
        }
        entries.push((col_idx, seg_id, bits));
    }

    // Sort by (col_idx, seg_id) for column-major insertion order, then
    // bulk-insert as multi-row VALUES (~100 rows/batch).
    entries.sort_by_key(|&(col_idx, seg_id, _)| (col_idx, seg_id));
    let batch_size = 100;
    for chunk in entries.chunks(batch_size) {
        let mut values: Vec<String> = Vec::with_capacity(chunk.len());
        for (col_idx, seg_id, bits) in chunk {
            // Hex-encode bytes as `'\x...'::bytea`.
            let hex: String = bits.iter().map(|b| format!("{:02x}", b)).collect();
            values.push(format!("({}, {}, '\\x{}'::bytea)", col_idx, seg_id, hex));
        }
        let sql = format!(
            "INSERT INTO {} (_col_idx, _segment_id, _bits) VALUES {}",
            ddl.valbitmap_fqn,
            values.join(", ")
        );
        spi_exec(&sql);
    }
    // Note: valbitmap_ddl already includes PRIMARY KEY inline (the table is
    // tiny vs blobs/blooms, so the bulk-insert-then-add-PK optimization
    // those use isn't worth it here).
    spi_exec(&format!("ANALYZE {}", ddl.valbitmap_fqn));

    // Build the catalog payload: column name → sorted value list.
    let mut by_name: HashMap<String, Vec<String>> = HashMap::new();
    for (col_idx, (vals, _)) in finalized {
        if let Some(name) = col_idx_to_name.get(&col_idx) {
            by_name.insert(name.clone(), vals);
        }
    }
    by_name
}

/// Binary search for partition by time value.
fn find_partition(range_starts: &[i64], range_ends: &[i64], time_usec: i64) -> Option<usize> {
    // Partitions are sorted by range_start. Find the last partition where range_start <= time_usec
    let pos = range_starts.partition_point(|&start| start <= time_usec);
    if pos == 0 {
        return None;
    }
    let idx = pos - 1;
    if time_usec < range_ends[idx] {
        Some(idx)
    } else {
        None
    }
}

// ============================================================================
// Glob expansion
// ============================================================================

fn expand_file_glob(pattern: &str) -> Vec<String> {
    if pattern.contains('*') || pattern.contains('?') || pattern.contains('[') {
        let mut files: Vec<String> = glob::glob(pattern)
            .unwrap_or_else(|e| pgrx::error!("pg_deltax: invalid glob pattern: {}", e))
            .map(|r| {
                r.unwrap_or_else(|e| pgrx::error!("pg_deltax: glob error: {}", e))
                    .to_string_lossy()
                    .into_owned()
            })
            .collect();
        if files.is_empty() {
            pgrx::error!("pg_deltax: no files match pattern '{}'", pattern);
        }
        files.sort();
        files
    } else {
        vec![pattern.to_string()]
    }
}

// ============================================================================
// Parquet loading
// ============================================================================

fn handle_copy_from_parquet(
    filename: &str,
    state: &BackfillState,
    part_buffers: &mut [PartitionBuffer],
    range_starts: &[i64],
    range_ends: &[i64],
) {
    use parquet::file::reader::{FileReader, SerializedFileReader};
    use std::fs::File;

    let file = File::open(filename)
        .unwrap_or_else(|e| pgrx::error!("pg_deltax: cannot open '{}': {}", filename, e));
    let reader = SerializedFileReader::new(file)
        .unwrap_or_else(|e| pgrx::error!("pg_deltax: invalid parquet file '{}': {}", filename, e));
    let col_mapping = crate::copyparquet::map_parquet_to_pg_columns(
        reader.metadata().file_metadata().schema_descr(),
        &state.columns,
    )
    .unwrap_or_else(|e| pgrx::error!("{}", e));

    let mut file_rows: i64 = 0;
    let mut file_skipped: i64 = 0;
    let total_parquet_rows: i64 = (0..reader.metadata().num_row_groups())
        .map(|i| reader.metadata().row_group(i).num_rows())
        .sum();

    for rg_idx in 0..reader.metadata().num_row_groups() {
        let num_rows = reader.metadata().row_group(rg_idx).num_rows() as usize;
        if num_rows == 0 {
            continue;
        }
        let rg_reader = reader.get_row_group(rg_idx).unwrap_or_else(|e| {
            pgrx::error!("pg_deltax: failed to read row group {}: {}", rg_idx, e)
        });

        let typed_cols = crate::copyparquet::read_row_group_columns(
            &*rg_reader,
            &col_mapping,
            &state.kinds,
            num_rows,
            state.columns.len(),
        )
        .unwrap_or_else(|e| pgrx::error!("{}", e));

        let before = file_rows;
        route_rows_to_partitions(
            typed_cols,
            num_rows,
            state,
            part_buffers,
            range_starts,
            range_ends,
            &mut file_rows,
        );
        file_skipped += num_rows as i64 - (file_rows - before);
    }

    if file_skipped > 0 {
        pgrx::warning!(
            "pg_deltax: loaded {} rows, skipped {} rows (no matching partition) from parquet file '{}' ({} total in file)",
            file_rows,
            file_skipped,
            filename,
            total_parquet_rows
        );
    } else {
        pgrx::notice!(
            "pg_deltax: loaded {} rows from parquet file '{}'",
            file_rows,
            filename
        );
    }
}

/// A batch of rows pre-routed to a single partition by a worker thread.
struct RoutedBatch {
    partition_idx: usize,
    typed_cols: Vec<TypedColumn>,
    num_rows: usize,
}

/// Shared state for parallel parquet decode+route workers.
struct ParquetWorkerJob {
    files: Vec<String>,
    columns: Vec<ColumnMeta>,
    kinds: Vec<ColumnKind>,
    range_starts: Vec<i64>,
    range_ends: Vec<i64>,
    time_col_index: usize,
    n_partitions: usize,
    next_file: std::sync::atomic::AtomicUsize,
}

/// Decode one parquet file and route rows to per-partition batches (pure Rust, no PG calls).
/// Sends one RoutedBatch per non-empty partition per row group through the channel.
fn decode_and_route_parquet_file(
    filename: &str,
    job: &ParquetWorkerJob,
    tx: &std::sync::mpsc::SyncSender<Result<RoutedBatch, String>>,
) -> Result<(), String> {
    use parquet::file::reader::{FileReader, SerializedFileReader};
    use std::fs::File;

    let file = File::open(filename)
        .map_err(|e| format!("pg_deltax: cannot open '{}': {}", filename, e))?;
    let reader = SerializedFileReader::new(file)
        .map_err(|e| format!("pg_deltax: invalid parquet '{}': {}", filename, e))?;
    let col_mapping = crate::copyparquet::map_parquet_to_pg_columns(
        reader.metadata().file_metadata().schema_descr(),
        &job.columns,
    )?;

    for rg_idx in 0..reader.metadata().num_row_groups() {
        let num_rows = reader.metadata().row_group(rg_idx).num_rows() as usize;
        if num_rows == 0 {
            continue;
        }
        let rg_reader = reader
            .get_row_group(rg_idx)
            .map_err(|e| format!("pg_deltax: row group {} error: {}", rg_idx, e))?;
        let typed_cols = crate::copyparquet::read_row_group_columns(
            &*rg_reader,
            &col_mapping,
            &job.kinds,
            num_rows,
            job.columns.len(),
        )?;

        // Route rows to partitions (pure Rust)
        let time_vals = match &typed_cols[job.time_col_index] {
            TypedColumn::Int64(v) => v,
            _ => return Err("pg_deltax: time column must be Int64 (timestamp)".to_string()),
        };

        // Fast path: check if entire batch fits in one partition
        let mut min_ts = i64::MAX;
        let mut max_ts = i64::MIN;
        for &ts in time_vals.iter().take(num_rows).flatten() {
            if ts < min_ts {
                min_ts = ts;
            }
            if ts > max_ts {
                max_ts = ts;
            }
        }

        let min_part = find_partition(&job.range_starts, &job.range_ends, min_ts);
        let max_part = find_partition(&job.range_starts, &job.range_ends, max_ts);

        if min_part == max_part {
            if let Some(part_idx) = min_part {
                let batch = RoutedBatch {
                    partition_idx: part_idx,
                    typed_cols,
                    num_rows,
                };
                if tx.send(Ok(batch)).is_err() {
                    return Ok(());
                }
            }
            continue;
        }

        // Slow path: scatter rows into per-partition buffers
        let mut part_cols: Vec<Option<Vec<TypedColumn>>> =
            (0..job.n_partitions).map(|_| None).collect();
        let mut part_counts: Vec<usize> = vec![0; job.n_partitions];

        for (row, ts_opt) in time_vals.iter().enumerate().take(num_rows) {
            let ts = ts_opt.unwrap_or(0);
            if let Some(part_idx) = find_partition(&job.range_starts, &job.range_ends, ts) {
                let cols = part_cols[part_idx].get_or_insert_with(|| {
                    job.kinds.iter().map(|k| new_typed_column(*k)).collect()
                });
                for (i, src_col) in typed_cols.iter().enumerate() {
                    cols[i].push_from(src_col, row);
                }
                part_counts[part_idx] += 1;
            }
        }

        // Send one RoutedBatch per non-empty partition
        for (part_idx, cols_opt) in part_cols.into_iter().enumerate() {
            if let Some(cols) = cols_opt {
                let batch = RoutedBatch {
                    partition_idx: part_idx,
                    typed_cols: cols,
                    num_rows: part_counts[part_idx],
                };
                if tx.send(Ok(batch)).is_err() {
                    return Ok(());
                }
            }
        }
    }
    Ok(())
}

/// Three-stage parallel parquet loading pipeline:
///
/// ```text
/// Stage 1: Decode+Route workers (4 threads)
///     Read parquet files, decode row groups, route rows to partitions
///     → route_channel(4) →
///
/// Stage 2: Main thread accumulator
///     Receive routed batches, extend partition buffers
///     When buffer hits segment_size, ship typed_cols to compress pool
///     → compress_channel(4) →
///
/// Stage 3: Compress workers (remaining threads)
///     Sort, compute ndistinct/blooms, compress columns
///     → flush_channel(4) →
///
/// Stage 4: Main thread flusher
///     Receive CompressedSegments, heap_insert blobs (I/O only)
/// ```
///
/// Stages 2 and 4 share the main thread: the main thread multiplexes between
/// receiving routed batches and draining compressed segments ready for flush.
///
/// We use plain `std::thread::spawn` (not `thread::scope`) because the main
/// thread calls PG functions that may `longjmp` (via `pgrx::error!`). With
/// regular threads + Arc, a longjmp drops channels and workers exit cleanly.
fn handle_copy_from_parquet_parallel(
    files: &[String],
    state: &BackfillState,
    part_buffers: &mut [PartitionBuffer],
    range_starts: &[i64],
    range_ends: &[i64],
) {
    use std::sync::Arc;
    use std::sync::mpsc::sync_channel;

    let total_workers = crate::get_parallel_workers();
    // Split workers: up to 4 for decode+route, remainder for compress
    let n_decode = total_workers.min(files.len()).min(4);
    let n_compress = total_workers.saturating_sub(n_decode).max(1);

    let (route_tx, route_rx) = sync_channel::<Result<RoutedBatch, String>>(4);
    let (compress_tx, compress_rx) = sync_channel::<CompressedSegment>(4);

    let decode_job = Arc::new(ParquetWorkerJob {
        files: files.to_vec(),
        columns: state.columns.clone(),
        kinds: state.kinds.clone(),
        range_starts: range_starts.to_vec(),
        range_ends: range_ends.to_vec(),
        time_col_index: state.time_col_index,
        n_partitions: part_buffers.len(),
        next_file: std::sync::atomic::AtomicUsize::new(0),
    });

    // Shared state for compress workers
    let compress_columns: Arc<Vec<ColumnMeta>> = Arc::new(state.columns.clone());
    let compress_order: Arc<Vec<usize>> = Arc::new(state.order_col_indices.clone());
    let bloom_enabled = crate::BLOOM_FILTERS.get();

    // Channel for segments to compress: (typed_cols, row_count, seg_id, partition_idx)
    let (seg_tx, seg_rx) = sync_channel::<(Vec<TypedColumn>, usize, i32, usize)>(4);
    let seg_rx = Arc::new(std::sync::Mutex::new(seg_rx));

    pgrx::notice!(
        "pg_deltax: parallel parquet pipeline: {} decode + {} compress workers, {} files",
        n_decode,
        n_compress,
        files.len()
    );

    // Stage 1: Decode+route workers
    let mut handles = Vec::new();
    for _ in 0..n_decode {
        let tx = route_tx.clone();
        let job = Arc::clone(&decode_job);
        handles.push(std::thread::spawn(move || {
            loop {
                let idx = job.next_file.fetch_add(1, Ordering::Relaxed);
                if idx >= job.files.len() {
                    break;
                }
                if let Err(e) = decode_and_route_parquet_file(&job.files[idx], &job, &tx) {
                    let _ = tx.send(Err(e));
                    return;
                }
            }
        }));
    }
    drop(route_tx);

    // Stage 3: Compress workers
    for _ in 0..n_compress {
        let seg_rx = Arc::clone(&seg_rx);
        let compress_tx = compress_tx.clone();
        let columns = Arc::clone(&compress_columns);
        let order = Arc::clone(&compress_order);
        handles.push(std::thread::spawn(move || {
            loop {
                let (typed_cols, row_count, seg_id, partition_idx) = {
                    let rx = seg_rx.lock().unwrap();
                    match rx.recv() {
                        Ok(item) => item,
                        Err(_) => break, // channel closed
                    }
                };
                let cs = compress_segment(
                    typed_cols,
                    row_count,
                    seg_id,
                    partition_idx,
                    &columns,
                    &order,
                    bloom_enabled,
                    1,
                );
                if compress_tx.send(cs).is_err() {
                    return;
                }
            }
        }));
    }
    drop(compress_tx);

    // Helper: drain all ready compressed segments (non-blocking).
    // Must be called frequently to prevent deadlock: compress workers block on
    // compress_tx.send() if its channel is full, which prevents them from consuming
    // seg_rx, which prevents the main thread's seg_tx.try_send() from succeeding.
    fn drain_compressed(
        part_buffers: &mut [PartitionBuffer],
        compress_rx: &std::sync::mpsc::Receiver<CompressedSegment>,
        state: &BackfillState,
    ) {
        while let Ok(cs) = compress_rx.try_recv() {
            let pi = cs.partition_idx;
            write_compressed_segment(cs, &mut part_buffers[pi], state);
        }
    }

    // Helper: send a segment to the compress pool, draining compressed results
    // to avoid deadlock. If try_send fails because the channel is full, we drain
    // compressed results first, then block on compress_rx.recv() to guarantee
    // progress (avoids spinning at 100% CPU).
    type SegItem = (Vec<TypedColumn>, usize, i32, usize);
    fn send_to_compress(
        mut item: SegItem,
        seg_tx: &std::sync::mpsc::SyncSender<SegItem>,
        part_buffers: &mut [PartitionBuffer],
        compress_rx: &std::sync::mpsc::Receiver<CompressedSegment>,
        state: &BackfillState,
    ) {
        loop {
            match seg_tx.try_send(item) {
                Ok(()) => return,
                Err(std::sync::mpsc::TrySendError::Full(returned)) => {
                    item = returned;
                    // Drain any ready results first
                    drain_compressed(part_buffers, compress_rx, state);
                    // If still can't send, block until a compressed result arrives
                    match seg_tx.try_send(item) {
                        Ok(()) => return,
                        Err(std::sync::mpsc::TrySendError::Full(returned)) => {
                            item = returned;
                            if let Ok(cs) = compress_rx.recv() {
                                let pi = cs.partition_idx;
                                write_compressed_segment(cs, &mut part_buffers[pi], state);
                            }
                        }
                        Err(std::sync::mpsc::TrySendError::Disconnected(_)) => return,
                    }
                }
                Err(std::sync::mpsc::TrySendError::Disconnected(_)) => return,
            }
        }
    }

    // Stages 2+4: Main thread — accumulate routed batches and drain compressed segments
    let mut seg_tx = Some(seg_tx);
    // Per-partition segment ID counters (workers need unique IDs)
    let mut next_seg_ids: Vec<i32> = part_buffers.iter().map(|b| b.next_segment_id).collect();

    // Phase A: receive routed batches, interleave with draining compressed segments
    loop {
        // Drain any ready compressed segments
        drain_compressed(part_buffers, &compress_rx, state);

        // Receive next routed batch (blocking with short timeout to interleave flush)
        match route_rx.recv_timeout(std::time::Duration::from_millis(10)) {
            Ok(msg) => {
                let batch = msg.unwrap_or_else(|e| pgrx::error!("{}", e));
                let part_idx = batch.partition_idx;
                // Extend the partition buffer with the routed batch
                {
                    let pbuf = &mut part_buffers[part_idx];
                    for (i, col) in batch.typed_cols.into_iter().enumerate() {
                        pbuf.typed_cols[i].extend(col);
                    }
                    pbuf.row_count += batch.num_rows;
                }
                // Ship segment_size chunks to compress pool
                while part_buffers[part_idx].row_count >= state.segment_size {
                    let seg_size = state.segment_size;
                    // Split off remainder, keep exactly seg_size rows
                    let remainder: Vec<TypedColumn> = part_buffers[part_idx]
                        .typed_cols
                        .iter_mut()
                        .map(|col| col.split_off(seg_size))
                        .collect();
                    let typed_cols =
                        std::mem::replace(&mut part_buffers[part_idx].typed_cols, remainder);
                    part_buffers[part_idx].row_count -= seg_size;
                    let seg_id = next_seg_ids[part_idx];
                    next_seg_ids[part_idx] += 1;
                    send_to_compress(
                        (typed_cols, seg_size, seg_id, part_idx),
                        seg_tx.as_ref().unwrap(),
                        part_buffers,
                        &compress_rx,
                        state,
                    );
                }
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
        }
    }

    // Route channel closed — flush remaining partial segments to compress pool
    for part_idx in 0..part_buffers.len() {
        if part_buffers[part_idx].row_count > 0 {
            let typed_cols = std::mem::replace(
                &mut part_buffers[part_idx].typed_cols,
                init_typed_columns(&state.columns, &state.kinds),
            );
            let row_count = part_buffers[part_idx].row_count;
            part_buffers[part_idx].row_count = 0;
            let seg_id = next_seg_ids[part_idx];
            next_seg_ids[part_idx] += 1;
            send_to_compress(
                (typed_cols, row_count, seg_id, part_idx),
                seg_tx.as_ref().unwrap(),
                part_buffers,
                &compress_rx,
                state,
            );
        }
    }
    seg_tx.take(); // Drop sender to signal compress workers to finish

    // Phase B: drain remaining compressed segments (blocking)
    for cs in compress_rx {
        let pi = cs.partition_idx;
        write_compressed_segment(cs, &mut part_buffers[pi], state);
    }

    // Wait for all worker threads
    for h in handles {
        let _ = h.join();
    }
}

/// Route rows from a batch of TypedColumns to the appropriate partition buffers.
fn route_rows_to_partitions(
    typed_cols: Vec<TypedColumn>,
    num_rows: usize,
    state: &BackfillState,
    part_buffers: &mut [PartitionBuffer],
    range_starts: &[i64],
    range_ends: &[i64],
    total_rows: &mut i64,
) {
    let time_vals = match &typed_cols[state.time_col_index] {
        TypedColumn::Int64(v) => v,
        _ => pgrx::error!("pg_deltax: time column must be Int64 (timestamp)"),
    };

    // Fast path: check min/max timestamps to see if entire batch fits in one partition.
    // Scan all values (not just first/last) since parquet row groups may not be sorted.
    let mut min_ts = i64::MAX;
    let mut max_ts = i64::MIN;
    for &ts in time_vals.iter().take(num_rows).flatten() {
        if ts < min_ts {
            min_ts = ts;
        }
        if ts > max_ts {
            max_ts = ts;
        }
    }

    let min_part = find_partition(range_starts, range_ends, min_ts);
    let max_part = find_partition(range_starts, range_ends, max_ts);

    if min_part == max_part
        && let Some(part_idx) = min_part
    {
        // All rows fall in the same partition — bulk extend
        let pbuf = &mut part_buffers[part_idx];
        for (i, col) in typed_cols.into_iter().enumerate() {
            pbuf.typed_cols[i].extend(col);
        }
        pbuf.row_count += num_rows;
        *total_rows += num_rows as i64;
        if pbuf.row_count >= state.segment_size {
            flush_segment(pbuf, state);
        }
        return;
    }

    // Slow path: per-row scatter (multiple partitions, or some rows out of range)
    for (row, ts_opt) in time_vals.iter().enumerate().take(num_rows) {
        let ts = ts_opt.unwrap_or(0);
        if let Some(part_idx) = find_partition(range_starts, range_ends, ts) {
            let pbuf = &mut part_buffers[part_idx];
            for (i, col) in typed_cols.iter().enumerate() {
                pbuf.typed_cols[i].push_from(col, row);
            }
            pbuf.row_count += 1;
            *total_rows += 1;
            if pbuf.row_count >= state.segment_size {
                flush_segment(pbuf, state);
            }
        }
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use super::*;

    fn ranges(starts: &[i64], lens: &[i64]) -> (Vec<i64>, Vec<i64>) {
        let ends: Vec<i64> = starts.iter().zip(lens).map(|(s, l)| s + l).collect();
        (starts.to_vec(), ends)
    }

    #[test]
    fn find_partition_empty_ranges() {
        assert_eq!(find_partition(&[], &[], 100), None);
    }

    #[test]
    fn find_partition_before_first_range() {
        let (s, e) = ranges(&[100, 200, 300], &[100, 100, 100]);
        assert_eq!(find_partition(&s, &e, 50), None);
        assert_eq!(find_partition(&s, &e, 99), None);
    }

    #[test]
    fn find_partition_after_last_range() {
        let (s, e) = ranges(&[100, 200, 300], &[100, 100, 100]);
        // end is exclusive: 300 + 100 = 400, so 400 falls outside
        assert_eq!(find_partition(&s, &e, 400), None);
        assert_eq!(find_partition(&s, &e, 10_000), None);
    }

    #[test]
    fn find_partition_exact_start_is_inclusive() {
        let (s, e) = ranges(&[100, 200, 300], &[100, 100, 100]);
        assert_eq!(find_partition(&s, &e, 100), Some(0));
        assert_eq!(find_partition(&s, &e, 200), Some(1));
        assert_eq!(find_partition(&s, &e, 300), Some(2));
    }

    #[test]
    fn find_partition_exact_end_is_exclusive() {
        // end of [100, 200) is 200, which belongs to the NEXT partition.
        let (s, e) = ranges(&[100, 200, 300], &[100, 100, 100]);
        assert_eq!(find_partition(&s, &e, 199), Some(0));
        assert_eq!(find_partition(&s, &e, 200), Some(1));
        // last partition's end (400) is past the end of all ranges
        assert_eq!(find_partition(&s, &e, 399), Some(2));
        assert_eq!(find_partition(&s, &e, 400), None);
    }

    #[test]
    fn find_partition_with_gaps() {
        // gaps between ranges: [100,200), [300,400), [500,600)
        let s = vec![100, 300, 500];
        let e = vec![200, 400, 600];
        assert_eq!(find_partition(&s, &e, 150), Some(0));
        assert_eq!(find_partition(&s, &e, 200), None); // in gap
        assert_eq!(find_partition(&s, &e, 250), None);
        assert_eq!(find_partition(&s, &e, 300), Some(1));
        assert_eq!(find_partition(&s, &e, 450), None); // in gap
        assert_eq!(find_partition(&s, &e, 599), Some(2));
    }

    #[test]
    fn find_partition_single_range() {
        let s = vec![1000];
        let e = vec![2000];
        assert_eq!(find_partition(&s, &e, 500), None);
        assert_eq!(find_partition(&s, &e, 1000), Some(0));
        assert_eq!(find_partition(&s, &e, 1500), Some(0));
        assert_eq!(find_partition(&s, &e, 1999), Some(0));
        assert_eq!(find_partition(&s, &e, 2000), None);
    }

    #[test]
    fn find_partition_negative_timestamps() {
        // pg_deltax internally stores Unix-epoch usec which can be negative
        // for pre-1970 data.
        let s = vec![-2000, -1000, 0];
        let e = vec![-1000, 0, 1000];
        assert_eq!(find_partition(&s, &e, -1500), Some(0));
        assert_eq!(find_partition(&s, &e, -500), Some(1));
        assert_eq!(find_partition(&s, &e, 500), Some(2));
        assert_eq!(find_partition(&s, &e, -3000), None);
    }

    #[test]
    fn expand_file_glob_literal_returns_singleton() {
        // No glob meta-chars → returned verbatim, no FS access.
        let v = expand_file_glob("/some/literal/path.tsv");
        assert_eq!(v, vec!["/some/literal/path.tsv".to_string()]);
    }
}
