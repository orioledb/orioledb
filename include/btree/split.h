/*-------------------------------------------------------------------------
 *
 * split.h
 *		Declarations for splitting B-tree pages.
 *
 * Copyright (c) 2021-2025, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/btree/split.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __BTREE_SPLIT_H__
#define __BTREE_SPLIT_H__

#include "btree.h"
#include "btree/find.h"
#include "btree/page_chunks.h"

typedef struct
{
	BTreePageItem items[BTREE_PAGE_MAX_SPLIT_ITEMS];
	int			itemsCount;
	int			hikeySize;
	int			maxKeyLen;
	int			hikeysEnd;
	bool		leaf;
} BTreeSplitItems;

extern void make_split_items(BTreeDescr *desc, Page page,
							 BTreeSplitItems *items,
							 OffsetNumber *offset, Pointer tupleheader,
							 OTuple tuple, LocationIndex tuplesize,
							 bool replace, CommitSeqNo csn);
extern void perform_page_compaction(BTreeDescr *desc, OInMemoryBlkno blkno,
									BTreeSplitItems *items, bool needsUndo,
									CommitSeqNo csn);
extern void perform_page_split(BTreeDescr *desc, OInMemoryBlkno blkno,
							   OInMemoryBlkno new_blkno,
							   BTreeSplitItems *items,
							   OffsetNumber left_count,
							   OTuple splitkey, LocationIndex splitkey_len,
							   CommitSeqNo csn, UndoLocation undoLoc);
extern OffsetNumber btree_page_split_location(BTreeDescr *desc,
											  BTreeSplitItems *items,
											  OffsetNumber targetLocation,
											  float4 spaceRatio,
											  OTuple *split_item);
OffsetNumber btree_get_split_left_count(BTreeDescr *desc, Page page,
										OffsetNumber offset, bool replace,
										BTreeSplitItems *items,
										OTuple *split_key,
										LocationIndex *split_key_len);

#endif							/* __BTREE_SPLIT_H__ */
