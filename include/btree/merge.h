/*-------------------------------------------------------------------------
 *
 * merge.h
 * 		Declarations for B-tree pages merge.
 *
 * Copyright (c) 2021-2022, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/btree/merge.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __BTREE_MERGE_H__
#define __BTREE_MERGE_H__

#include "btree.h"

extern bool btree_try_merge_pages(BTreeDescr *desc,
								  OInMemoryBlkno parent_blkno,
								  OFixedKey *parent_hikey,
								  bool *merge_parent,
								  OInMemoryBlkno left_blkno,
								  BTreePageItemLocator right_loc,
								  OInMemoryBlkno right_blkno);
extern bool btree_try_merge_and_unlock(BTreeDescr *desc, OInMemoryBlkno blkno,
									   bool nested, bool wait_io);
extern bool is_page_too_sparse(BTreeDescr *desc, Page p);

#endif							/* __BTREE_MERGE_H__ */
