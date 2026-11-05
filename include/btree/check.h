/*-------------------------------------------------------------------------
 *
 * check.h
 *		Declarations for checking the OrioleDB B-tree structure.
 *
 * Copyright (c) 2021-2026, Oriole DB Inc.
 * Copyright (c) 2025-2026, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/btree/check.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __BTREE_CHECK_H__
#define __BTREE_CHECK_H__

#include "btree/btree.h"
#include "tableam/descr.h"

typedef struct
{
	int			from;
	int			to;
	int			leaf_count;
	int			node_count;
} BTreeCompressRange;

typedef struct
{
	int			errors;
	int			oversize;
	int			nranges;
	int64		totalSize;
	int64		totalCompressedSize;
	BTreeCompressRange *ranges;
} BTreeCompressStats;

typedef void (*BTreeCheckReport) (const char *msg);

extern bool check_btree(BTreeDescr *desc, bool force_file_check,
						bool wait_for_checkpoint);
extern void check_btree_structure(BTreeDescr *desc, const char *name,
								  bool parentcheck, bool rootdescend,
								  BTreeCheckReport report,
								  bool heapallindexed,
								  OIndexDescr *secondaryIndex,
								  OIndexDescr *primaryIndex,
								  OTableDescr *tableDescr);
extern void check_btree_compression(BTreeDescr *desc,
									BTreeCompressStats *stats,
									OCompress lvl);

#endif							/* __BTREE_CHECK_H__ */
