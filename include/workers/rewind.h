/*-------------------------------------------------------------------------
 *
 * rewind.h
 *		Routines for background writer process.
 *
 * Copyright (c) 2021-2025, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/workers/rewind.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __REWIND_WORKER_H__
#define __REWIND_WORKER_H__

//extern bool IsBGWriter;

extern void register_rewind_worker(void);
PGDLLEXPORT void rewind_worker_main(Datum);

#endif							/* __REWIND_WORKER_H__ */
