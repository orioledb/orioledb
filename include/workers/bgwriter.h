/*-------------------------------------------------------------------------
 *
 * bgwriter.h
 *		Routines for background writer process.
 *
 * Copyright (c) 2021-2025, Oriole DB Inc.
 * Copyright (c) 2025, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/utils/bgwriter.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __BGWRITER_H__
#define __BGWRITER_H__

extern bool IsBGWriter;

extern void register_bgwriter(void);
PGDLLEXPORT void bgwriter_main(Datum);

#endif							/* __BGWRITER_H__ */
