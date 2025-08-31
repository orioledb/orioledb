/*-------------------------------------------------------------------------
 *
 * tree.h
 *		Declarations for implementation of BTree interface for OrioleDB
 *		tables.
 *
 * Copyright (c) 2021-2025, Oriole DB Inc.
 * Copyright (c) 2025, Supabase Inc.
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

extern void index_btree_desc_init(BTreeDescr *desc, OCompress compress, int fillfactor,
								  ORelOids oids, OIndexType type,
								  char persistence, OXid createOxid,
								  void *arg);
extern uint32 o_hash_iptr(OIndexDescr *idx, ItemPointer iptr);
extern void o_fill_key_bound(OIndexDescr *id, OTuple tuple,
							 BTreeKeyType keyType, OBTreeKeyBound *bound);
extern void o_fill_bridge_index_key_bound(BTreeDescr *secondary, OTuple tuple, OBTreeKeyBound *bound);
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
