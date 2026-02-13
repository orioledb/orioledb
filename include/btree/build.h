/*-------------------------------------------------------------------------
 *
 * build.h
 * 		Declarations for sort-based B-tree index building.
 *
 * Copyright (c) 2021-2026, Oriole DB Inc.
 * Copyright (c) 2025-2026, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/btree/build.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __BTREE_BUILD_H__
#define __BTREE_BUILD_H__

#include "btree.h"

typedef struct Tuplesortstate Tuplesortstate;

extern void btree_write_index_data(BTreeDescr *desc, TupleDesc tupdesc,
								   Tuplesortstate *sortstate,
								   uint64 ctid, uint64 bridge_ctid,
								   CheckpointFileHeader *file_header);
extern S3TaskLocation btree_write_file_header(BTreeDescr *desc,
											  CheckpointFileHeader *file_header);

#endif							/* __BTREE_BUILD_H__ */
