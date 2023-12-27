/*-------------------------------------------------------------------------
 *
 * tree.h
 *		Declarations for implementation of BTree interface for OrioleDB
 *		tables.
 *
 * Copyright (c) 2021-2023, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/tableam/tree.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __TABLEAM_TREE_H__
#define __TABLEAM_TREE_H__

#include "tableam/descr.h"
#include "tableam/key_range.h"

extern void index_btree_desc_init(BTreeDescr *desc, OCompress compress,
								  ORelOids oids, OIndexType type,
								  char persistence, OXid createOxid,
								  void *arg);
extern void o_fill_key_bound(OIndexDescr *id, OTuple tuple,
							 BTreeKeyType keyType, OBTreeKeyBound *bound);
extern void o_fill_secondary_key_bound(BTreeDescr *primary,
									   BTreeDescr *secondary,
									   OTuple tuple, TupleTableSlot *slot,
									   OBTreeKeyBound *bound);
extern void o_fill_pindex_tuple_key_bound(BTreeDescr *desc,
										  OTuple tup, OBTreeKeyBound *bound);
extern int	o_idx_cmp_value_bounds(OBTreeValueBound *bound1,
								   OBTreeValueBound *bound2,
								   OIndexField *field,
								   bool *equal);
extern int	o_idx_cmp(BTreeDescr *desc,
					  void *p1, BTreeKeyType keyType1,
					  void *p2, BTreeKeyType keyType2);
extern int	o_idx_cmp_range_key_to_value(OBTreeValueBound *sk1, OIndexField *field,
										 Datum value, bool isnull);

#endif
