/*-------------------------------------------------------------------------
 *
 * bgwriter.h
 *		Routines for background writer process.
 *
 * Copyright (c) 2021-2026, Oriole DB Inc.
 * Copyright (c) 2025-2026, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/workers/bgwriter.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __BGWRITER_H__
#define __BGWRITER_H__

extern bool IsBGWriter;
extern int	BGWriterNum;

extern void register_bgwriter(int num);
PGDLLEXPORT void bgwriter_main(Datum);

#endif							/* __BGWRITER_H__ */
