/*-------------------------------------------------------------------------
 *
 * fault_injection.c
 *		Stress-test fault injection flags for OrioleDB.
 *
 * Copyright (c) 2025-2026, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/utils/fault_injection.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "utils/fault_injection.h"

#ifdef IS_DEV

#include "fmgr.h"

FaultInjectionState *fault_injection_state = NULL;

Size
fault_injection_shmem_needs(void)
{
	return sizeof(FaultInjectionState);
}

void
fault_injection_shmem_init(Pointer ptr, bool found)
{
	fault_injection_state = (FaultInjectionState *) ptr;

	if (!found)
		pg_atomic_init_u32(&fault_injection_state->wal_error_enabled, 0);
}

PG_FUNCTION_INFO_V1(orioledb_inject_wal_error);

Datum
orioledb_inject_wal_error(PG_FUNCTION_ARGS)
{
	bool		enable = PG_GETARG_BOOL(0);

	if (fault_injection_state == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("orioledb fault-injection shmem not initialized")));

	pg_atomic_write_u32(&fault_injection_state->wal_error_enabled, enable);

	PG_RETURN_VOID();
}

#endif							/* IS_DEV */
