/*-------------------------------------------------------------------------
 *
 * fault_injection.h
 *		Stress-test fault injection flags for OrioleDB.
 *
 * Shared-memory toggles that cause hot-path helpers to raise
 * ereport(ERROR, ...). Used from stress tests to drive the abort
 * path concurrently with live R/W workloads.
 *
 * Copyright (c) 2025-2026, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/utils/fault_injection.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __FAULT_INJECTION_H__
#define __FAULT_INJECTION_H__

#ifdef IS_DEV

#include "c.h"
#include "port/atomics.h"

typedef struct FaultInjectionState
{
	pg_atomic_uint32 wal_error_enabled;
} FaultInjectionState;

extern FaultInjectionState *fault_injection_state;

extern Size fault_injection_shmem_needs(void);
extern void fault_injection_shmem_init(Pointer ptr, bool found);

static inline bool
fault_injection_wal_error_enabled(void)
{
	return fault_injection_state != NULL &&
		pg_atomic_read_u32(&fault_injection_state->wal_error_enabled) != 0;
}

#endif							/* IS_DEV */

#endif							/* __FAULT_INJECTION_H__ */
