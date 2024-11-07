/*-------------------------------------------------------------------------
 *
 * handler.h
 *		Declarations of index access method handler
 *
 * Copyright (c) 2024, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/indexam/handler.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __INDEXAM_HANDLER_H__
#define __INDEXAM_HANDLER_H__

#include "access/amapi.h"

extern IndexAmRoutine *orioledb_indexam_routine_hook(Oid tamoid,
													 Oid amhandler);
extern IndexAmRoutine *orioledb_hash_indexam_handler(void);
extern IndexAmRoutine *orioledb_gist_indexam_handler(void);
extern IndexAmRoutine *orioledb_spgist_indexam_handler(void);
extern IndexAmRoutine *orioledb_gin_indexam_handler(void);
extern IndexAmRoutine *orioledb_brin_indexam_handler(void);

#endif
