/*-------------------------------------------------------------------------
 *
 * interrupt.h
 *		Routines for background workers interrupt handling.
 *
 * Copyright (c) 2021-2026, Oriole DB Inc.
 * Copyright (c) 2025-2026, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/workers/interrupt.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __WORKERS_INTERRUPT_H__
#define __WORKERS_INTERRUPT_H__

#include "miscadmin.h"
#include "postmaster/startup.h"

extern void o_worker_handle_interrupts(void);

#if PG_VERSION_NUM >= 180000
#define O_STARTUP_PROC_INTERRUPTS() ProcessStartupProcInterrupts()
#else
#define O_STARTUP_PROC_INTERRUPTS() HandleStartupProcInterrupts()
#endif

/*
 * Like CHECK_FOR_INTERRUPTS(), but backend-aware: the startup process and
 * orioledb's own background workers (recovery/S3/bgwriter workers) redefine
 * signal handling and don't respond to a plain CHECK_FOR_INTERRUPTS().
 */
#define O_CHECK_FOR_INTERRUPTS() \
	do { \
		if (AmStartupProcess()) \
			O_STARTUP_PROC_INTERRUPTS(); \
		else if (MyBackendType == B_BG_WORKER) \
			o_worker_handle_interrupts(); \
		else \
			CHECK_FOR_INTERRUPTS(); \
	} while (0)

#endif							/* __WORKERS_INTERRUPT_H__ */
