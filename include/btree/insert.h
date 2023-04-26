/*-------------------------------------------------------------------------
 *
 * insert.h
 *		Declarations for inserting tuples into B-tree.
 *
 * Copyright (c) 2021-2022, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/btree/insert.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __BTREE_INSERT_H__
#define __BTREE_INSERT_H__

#include "btree.h"
#include "btree/find.h"

extern void o_btree_split_fix_and_unlock(BTreeDescr *descr,
										 OInMemoryBlkno left_blkno);
extern void o_btree_insert_tuple_to_leaf(OBTreeFindPageContext *context,
										 OTuple tuple, LocationIndex tuplen,
										 BTreeLeafTuphdr *leaf_header,
										 bool replace,
										 int reserve_kind);
extern bool o_btree_split_is_incomplete(OInMemoryBlkno left_blkno,
										bool *relocked);

#endif							/* __BTREE_INSERT_H__ */
