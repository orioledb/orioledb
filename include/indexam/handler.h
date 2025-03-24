/*-------------------------------------------------------------------------
 *
 * handler.h
 *		Declarations of index access method handler
 *
 * Copyright (c) 2025-2025, Oriole DB Inc.
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
extern void o_add_bridged_option_to_ams(void);

extern Relation o_current_index;

#endif
