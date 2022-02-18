/*-------------------------------------------------------------------------
 *
 * split.h
 *		Declarations for splitting B-tree pages.
 *
 * Copyright (c) 2021-2022, Oriole DB Inc.
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

extern void perform_page_split(BTreeDescr *desc, OInMemoryBlkno blkno,
							   OInMemoryBlkno new_blkno,
							   OffsetNumber left_count, OTuple splitkey,
							   LocationIndex splitkey_size,
							   OffsetNumber *offset, bool *place_right,
							   Pointer tupleheader, OTuple tuple,
							   OffsetNumber tuplesize, bool replace,
							   CommitSeqNo csn, UndoLocation undoLoc);
extern OffsetNumber btree_page_split_location(BTreeDescr *desc, Page page,
											  OffsetNumber offset,
											  LocationIndex tuplesize,
											  OTuple tuple, bool replace,
											  OffsetNumber target_location,
											  float spaceRatio, OTuple *split_item,
											  CommitSeqNo csn);
extern OffsetNumber btree_get_split_left_count(BTreeDescr *desc,
											   OInMemoryBlkno blkno,
											   OTuple tuple,
											   LocationIndex tuplesize,
											   OffsetNumber offset,
											   bool replace,
											   OTuple *split_key,
											   LocationIndex *split_key_len,
											   CommitSeqNo csn);

#endif							/* __BTREE_SPLIT_H__ */
