/* Weak stubs for Postgres backend symbols referenced by the pgrx unit-test
 * binary on Linux x86_64. The dynamic loader otherwise refuses to launch the
 * test binary ("undefined symbol: CurrentMemoryContext"). Aarch64 happens to
 * fold the references out at link time, so the issue is invisible there.
 *
 * pgrx-tests runs `#[pg_test]` cases by spawning a real Postgres process and
 * dispatching SQL over a connection — the test binary itself never reads any
 * of these globals, so resolving them to 0 is safe.
 */

__attribute__((weak)) void *CurrentMemoryContext = 0;
__attribute__((weak)) void *ErrorContext = 0;
__attribute__((weak)) void *PG_exception_stack = 0;
__attribute__((weak)) void *TopMemoryContext = 0;
__attribute__((weak)) void *error_context_stack = 0;

__attribute__((weak)) void AllocSetContextCreateInternal(void) {}
__attribute__((weak)) void CopyErrorData(void) {}
__attribute__((weak)) void FreeErrorData(void) {}
__attribute__((weak)) void MemoryContextReset(void) {}
__attribute__((weak)) void OidInputFunctionCall(void) {}
__attribute__((weak)) void getTypeInputInfo(void) {}
__attribute__((weak)) void pg_detoast_datum(void) {}
