/* Weak stubs for Postgres backend symbols referenced by the pgrx unit-test
 * binary on Linux x86_64. The dynamic loader otherwise refuses to launch the
 * test binary ("undefined symbol: <name>"). Aarch64 happens to fold the
 * references out at link time, so the issue is invisible there.
 *
 * pgrx-tests runs `#[pg_test]` cases by spawning a real Postgres process and
 * dispatching SQL over a connection — the test binary itself never reads any
 * of these globals or invokes any of these functions, so resolving them to
 * 0 / no-op is safe. If a regular `#[test]` ever does call into one of these,
 * the test would simply observe 0 instead of crashing — caller's problem to
 * mock or convert back to `#[pg_test]`.
 *
 * The list is derived from `grep -rho 'pg_sys::[A-Za-z_][a-zA-Z0-9_]*' src/`
 * — when adding new pg_sys functions/globals, extend this file accordingly.
 */

/* Data globals (hooks + memory contexts + GUC-backing variables). */
__attribute__((weak)) void *create_upper_paths_hook = 0;
__attribute__((weak)) void *CurrentMemoryContext = 0;
__attribute__((weak)) void *ErrorContext = 0;
__attribute__((weak)) void *error_context_stack = 0;
__attribute__((weak)) void *get_relation_info_hook = 0;
__attribute__((weak)) int   max_parallel_workers_per_gather = 0;
__attribute__((weak)) char  pgBufferUsage[256] = {0};
__attribute__((weak)) void *PG_exception_stack = 0;
__attribute__((weak)) void *planner_hook = 0;
__attribute__((weak)) void *set_rel_pathlist_hook = 0;
__attribute__((weak)) void *shmem_request_hook = 0;
__attribute__((weak)) void *shmem_startup_hook = 0;
__attribute__((weak)) void *TopMemoryContext = 0;

/* Functions: no-op stubs. Signatures are reduced to void(void) because the
 * test binary never invokes them — only the dynamic loader walks the
 * relocation table at load time. */
__attribute__((weak)) void add_partial_path(void) {}
__attribute__((weak)) void add_path(void) {}
__attribute__((weak)) void AllocSetContextCreateInternal(void) {}
__attribute__((weak)) void BeginCopyFrom(void) {}
__attribute__((weak)) void bms_next_member(void) {}
__attribute__((weak)) void CacheInvalidateRelcacheByRelid(void) {}
__attribute__((weak)) void CopyErrorData(void) {}
__attribute__((weak)) void copyObjectImpl(void) {}
__attribute__((weak)) void create_agg_path(void) {}
__attribute__((weak)) void create_gather_path(void) {}
__attribute__((weak)) void cstring_to_text_with_len(void) {}
__attribute__((weak)) void deconstruct_array(void) {}
__attribute__((weak)) void defGetString(void) {}
__attribute__((weak)) void dsa_allocate_extended(void) {}
__attribute__((weak)) void dsa_attach_in_place(void) {}
__attribute__((weak)) void dsa_create_in_place(void) {}
__attribute__((weak)) void dsa_create_in_place_ext(void) {}
__attribute__((weak)) void dsa_detach(void) {}
__attribute__((weak)) void dsa_free(void) {}
__attribute__((weak)) void dsa_get_address(void) {}
__attribute__((weak)) void dsa_pin(void) {}
__attribute__((weak)) void dsa_pin_mapping(void) {}
__attribute__((weak)) void dsa_set_size_limit(void) {}
__attribute__((weak)) void EndCopyFrom(void) {}
__attribute__((weak)) void ExecClearTuple(void) {}
__attribute__((weak)) void ExecDropSingleTupleTableSlot(void) {}
__attribute__((weak)) void ExecStoreVirtualTuple(void) {}
__attribute__((weak)) void ExplainPropertyText(void) {}
__attribute__((weak)) void exprCollation(void) {}
__attribute__((weak)) void exprType(void) {}
__attribute__((weak)) void extract_actual_clauses(void) {}
__attribute__((weak)) void fetch_upper_rel(void) {}
__attribute__((weak)) void free_parsestate(void) {}
__attribute__((weak)) void FreeBulkInsertState(void) {}
__attribute__((weak)) void FreeErrorData(void) {}
__attribute__((weak)) void get_attname(void) {}
__attribute__((weak)) void get_attnum(void) {}
__attribute__((weak)) void get_atttype(void) {}
__attribute__((weak)) void get_atttypetypmodcoll(void) {}
__attribute__((weak)) void get_func_name(void) {}
__attribute__((weak)) void get_namespace_name(void) {}
__attribute__((weak)) void get_namespace_oid(void) {}
__attribute__((weak)) void get_opname(void) {}
__attribute__((weak)) void get_rel_name(void) {}
__attribute__((weak)) void get_rel_namespace(void) {}
__attribute__((weak)) void get_relname_relid(void) {}
__attribute__((weak)) void get_sortgroupclause_tle(void) {}
__attribute__((weak)) void get_typlenbyval(void) {}
__attribute__((weak)) void get_typlenbyvalalign(void) {}
__attribute__((weak)) void GetActiveSnapshot(void) {}
__attribute__((weak)) void GetBulkInsertState(void) {}
__attribute__((weak)) void GetCurrentCommandId(void) {}
__attribute__((weak)) void getTypeInputInfo(void) {}
__attribute__((weak)) void getTypeOutputInfo(void) {}
__attribute__((weak)) void heap_deform_tuple(void) {}
__attribute__((weak)) void heap_form_tuple(void) {}
__attribute__((weak)) void heap_freetuple(void) {}
__attribute__((weak)) void heap_getnext(void) {}
__attribute__((weak)) void heap_insert(void) {}
__attribute__((weak)) void index_beginscan(void) {}
__attribute__((weak)) void index_close(void) {}
__attribute__((weak)) void index_endscan(void) {}
__attribute__((weak)) void index_getnext_slot(void) {}
__attribute__((weak)) void index_open(void) {}
__attribute__((weak)) void index_rescan(void) {}
__attribute__((weak)) void lappend(void) {}
__attribute__((weak)) void lappend_int(void) {}
__attribute__((weak)) void lappend_oid(void) {}
__attribute__((weak)) void list_free(void) {}
__attribute__((weak)) void list_nth(void) {}
__attribute__((weak)) void list_nth_int(void) {}
__attribute__((weak)) void list_nth_oid(void) {}
__attribute__((weak)) void LWLockAcquire(void) {}
__attribute__((weak)) void LWLockInitialize(void) {}
__attribute__((weak)) void LWLockNewTrancheId(void) {}
__attribute__((weak)) void LWLockRegisterTranche(void) {}
__attribute__((weak)) void LWLockRelease(void) {}
__attribute__((weak)) void make_ands_implicit(void) {}
__attribute__((weak)) void make_parsestate(void) {}
__attribute__((weak)) void makeConst(void) {}
__attribute__((weak)) void makeDefElem(void) {}
__attribute__((weak)) void makeNullConst(void) {}
__attribute__((weak)) void makeString(void) {}
__attribute__((weak)) void makeTargetEntry(void) {}
__attribute__((weak)) void makeVar(void) {}
__attribute__((weak)) void MemoryContextDelete(void) {}
__attribute__((weak)) void MemoryContextReset(void) {}
__attribute__((weak)) void MemoryContextSetParent(void) {}
__attribute__((weak)) void MemoryContextSwitchTo(void) {}
__attribute__((weak)) void NextCopyFromRawFields(void) {}
__attribute__((weak)) void nodeToString(void) {}
__attribute__((weak)) void ObjectIdGetDatum(void) {}
__attribute__((weak)) void OidFunctionCall1Coll(void) {}
__attribute__((weak)) void OidFunctionCall2Coll(void) {}
__attribute__((weak)) void OidFunctionCall3Coll(void) {}
__attribute__((weak)) void OidInputFunctionCall(void) {}
__attribute__((weak)) void OidOutputFunctionCall(void) {}
__attribute__((weak)) void palloc(void) {}
__attribute__((weak)) void palloc0(void) {}
__attribute__((weak)) void pfree(void) {}
__attribute__((weak)) void pg_detoast_datum(void) {}
__attribute__((weak)) void pstrdup(void) {}
__attribute__((weak)) void pull_var_clause(void) {}
__attribute__((weak)) void pull_varattnos(void) {}
__attribute__((weak)) void RangeVarGetRelidExtended(void) {}
__attribute__((weak)) void RegisterCustomScanMethods(void) {}
__attribute__((weak)) void relation_close(void) {}
__attribute__((weak)) void relation_open(void) {}
__attribute__((weak)) void RelationGetIndexList(void) {}
__attribute__((weak)) void RelationGetNumberOfBlocksInFork(void) {}
__attribute__((weak)) void ReleaseSysCache(void) {}
__attribute__((weak)) void RequestAddinShmemSpace(void) {}
__attribute__((weak)) void ScanKeyInit(void) {}
__attribute__((weak)) void SearchSysCache1(void) {}
__attribute__((weak)) void ShmemInitStruct(void) {}
__attribute__((weak)) void slot_getallattrs(void) {}
__attribute__((weak)) void SPI_execute(void) {}
__attribute__((weak)) void SPI_freetuptable(void) {}
__attribute__((weak)) void SPI_getbinval(void) {}
__attribute__((weak)) void standard_ExecutorStart(void) {}
__attribute__((weak)) void standard_planner(void) {}
__attribute__((weak)) void standard_ProcessUtility(void) {}
__attribute__((weak)) void stringToNode(void) {}
__attribute__((weak)) void table_close(void) {}
__attribute__((weak)) void table_open(void) {}
__attribute__((weak)) void table_slot_create(void) {}
__attribute__((weak)) void text_to_cstring(void) {}
__attribute__((weak)) void varstr_cmp(void) {}

/* pgrx-internal panic/ereport plumbing — pgrx-pg-sys/src/submodules/panic.rs
 * declares these externs directly (not via the generated bindings), so they
 * don't show up in a grep of our src/ for `pg_sys::*`. They're reachable from
 * any panic path in the test binary. */
__attribute__((weak)) int  errcode(int sqlerrcode) { (void)sqlerrcode; return 0; }
__attribute__((weak)) int  errcontext_msg(const char *fmt, ...) { (void)fmt; return 0; }
__attribute__((weak)) int  errdetail(const char *fmt, ...) { (void)fmt; return 0; }
__attribute__((weak)) void errfinish(void) {}
__attribute__((weak)) int  errhint(const char *fmt, ...) { (void)fmt; return 0; }
__attribute__((weak)) int  errmsg(const char *fmt, ...) { (void)fmt; return 0; }
__attribute__((weak)) int  errstart(int elevel, const char *domain) { (void)elevel; (void)domain; return 0; }
__attribute__((weak)) void errstart_cold(void) {}
__attribute__((weak)) void pg_re_throw(void) {}
