/*-------------------------------------------------------------------------
 *
 * insert.h
 *		Declarations for inserting tuples into B-tree.
 *
 * Copyright (c) 2021-2026, Oriole DB Inc.
 * Copyright (c) 2025-2026, Supabase Inc.
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
#include "btree/modify.h"

extern void o_btree_split_fix_and_unlock(BTreeDescr *descr,
										 OInMemoryBlkno left_blkno);
extern void o_btree_split_fix_for_right_page_and_unlock(BTreeDescr *desc,
														OInMemoryBlkno rightBlkno);
extern void o_btree_insert_tuple_to_leaf(OBTreeFindPageContext *context,
										 OTuple tuple, LocationIndex tuplen,
										 BTreeLeafTuphdr *leaf_header,
										 bool replace,
										 int reserve_kind);
extern bool o_btree_split_is_incomplete(OInMemoryBlkno left_blkno,
										uint32 pageChangeCount,
										bool *relocked);

/*
 * Result of probing a leaf slot for a new-item insert:
 * Fits means the located position can receive the item AsIs.
 * HikeyCrossed -> the key belongs to a later leaf;
 * NoFit -> page is out of slack at this locator;
 * Duplicate -> the key already exists at the locator.
 *
 * The probe only detects "key past this leaf's hikey", not "key before
 * this leaf's lokey" -- detecting the latter would need parent context
 * (a leaf's downlink key is held by its parent, not the leaf itself).
 * The helper assumes keys[] are sorted ascending.
 *
 * For the batch driver Fits at the end of the loop means "no bail
 * happened, all items inserted"; caller need to inspect result only
 * when count < nitems.
 */
typedef enum BTreeLeafProbeResult
{
	BTreeLeafProbeFits,
	BTreeLeafProbeHikeyCrossed,
	BTreeLeafProbeNoFit,
	BTreeLeafProbeDuplicate
} BTreeLeafProbeResult;

extern int	o_btree_multi_insert_item(OBTreeFindPageContext *ctx,
									  OTuple *tuples,
									  LocationIndex *tuplens,
									  Pointer *keys,
									  BTreeKeyType keyType,
									  int nitems,
									  OXid opOxid,
									  RowLockMode lockMode,
									  BTreeModifyCallbackInfo *cb,
									  void **cb_args,
									  BTreeLeafProbeResult *result);

#endif							/* __BTREE_INSERT_H__ */
