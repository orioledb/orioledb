/*-------------------------------------------------------------------------
 *
 * o_tablespaces.h
 * 		Declarations for tablespace data directory routines.
 *
 * Copyright (c) 2021-2026, Oriole DB Inc.
 * Copyright (c) 2025-2026, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/catalog/o_tablespaces.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __O_TABLESPACES_H__
#define __O_TABLESPACES_H__

extern void o_get_prefixes_for_tablespace(Oid datoid, Oid tablespace,
										  char **prefix, char **db_prefix);

#endif							/* __O_TABLESPACES_H__ */
