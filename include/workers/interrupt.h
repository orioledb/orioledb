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

/*
 * Like CHECK_FOR_INTERRUPTS(), but backend-aware: the startup process and
 * orioledb's own background workers (recovery/S3/bgwriter workers) redefine
 * signal handling and don't respond to a plain CHECK_FOR_INTERRUPTS().
 */
extern void o_worker_handle_interrupts(void);

#endif							/* __WORKERS_INTERRUPT_H__ */
