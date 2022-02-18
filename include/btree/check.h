/*-------------------------------------------------------------------------
 *
 * check.h
 *		Declarations for checking the OrioleDB B-tree structure.
 *
 * Copyright (c) 2021-2022, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/btree/check.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __BTREE_CHECK_H__
#define __BTREE_CHECK_H__

#include "btree/btree.h"

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

extern bool check_btree(BTreeDescr *desc, bool force_file_check);
extern void check_btree_compression(BTreeDescr *desc,
									BTreeCompressStats *stats,
									OCompress lvl);

#endif							/* __BTREE_CHECK_H__ */
