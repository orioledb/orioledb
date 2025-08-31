/*-------------------------------------------------------------------------
 *
 * interrupt.h
 *		Routines for background workers interrupt handling.
 *
 * Copyright (c) 2021-2025, Oriole DB Inc.
 * Copyright (c) 2025, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/workers/interrupt.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __WORKERS_INTERRUPT_H__
#define __WORKERS_INTERRUPT_H__

extern void o_worker_shutdown(int elevel);
extern void o_worker_handle_interrupts(void);

#endif							/* __WORKERS_INTERRUPT_H__ */
