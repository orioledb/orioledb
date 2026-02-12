/*-------------------------------------------------------------------------
 *
 * vacuum.h
 *		Declarations for implementation of BTree interface for OrioleDB
 *		tables.
 *
 * Copyright (c) 2025-2026, Oriole DB Inc.
 * Copyright (c) 2025-2026, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/tableam/vacuum.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __TABLEAM_VACUUM_H__
#define __TABLEAM_VACUUM_H__

#include "access/tableam.h"

extern void orioledb_vacuum_bridged_indexes(Relation rel, OTableDescr *descr,
											struct VacuumParams *params,
											BufferAccessStrategy bstrategy);

#endif							/* __TABLEAM_VACUUM_H__ */
