/*-------------------------------------------------------------------------
 *
 * iterator.h
 *		Declarations of orioledb B-tree iterator.
 *
 * Copyright (c) 2021-2022, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/btree/iterator.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __BTREE_ITERATOR_H__
#define __BTREE_ITERATOR_H__

#include "btree.h"

typedef enum
{
	OTupleFetchNext,
	OTupleFetchMatch,
	OTupleFetchNotMatch
} TupleFetchCallbackResult;

typedef enum
{
	OTupleFetchCallbackVersionCheck,
	OTupleFetchCallbackKeyCheck
} TupleFetchCallbackCheckType;

typedef TupleFetchCallbackResult (*TupleFetchCallback) (OTuple tuple,
														OXid tupOxid, CommitSeqNo csn, void *arg,
														TupleFetchCallbackCheckType check_type);

extern OTuple o_btree_find_tuple_by_key(BTreeDescr *desc, void *key,
										BTreeKeyType kind,
										CommitSeqNo readCsn,
										CommitSeqNo *outCsn,
										MemoryContext mcxt,
										BTreeLocationHint *hint);

extern BTreeIterator *o_btree_iterator_create(BTreeDescr *desc, void *key,
											  BTreeKeyType kind, CommitSeqNo csn,
											  ScanDirection scan);
extern void o_btree_iterator_set_tuple_ctx(BTreeIterator *it,
										   MemoryContext tupleCxt);
extern void o_btree_iterator_set_callback(BTreeIterator *it,
										  TupleFetchCallback callback,
										  void *arg);
extern OTuple o_btree_iterator_fetch(BTreeIterator *it, CommitSeqNo *tupleCsn,
									 void *end, BTreeKeyType endType,
									 bool endIsIncluded,
									 BTreeLocationHint *hint);
extern OTuple btree_iterate_raw(BTreeIterator *it, void *end,
								BTreeKeyType endKind, bool endInclude,
								bool *scanEnd, BTreeLocationHint *hint);
extern void btree_iterator_free(BTreeIterator *it);

extern OTuple o_btree_find_tuple_by_key_cb(BTreeDescr *desc, void *key,
										   BTreeKeyType kind,
										   CommitSeqNo readCsn,
										   CommitSeqNo *outCsn,
										   MemoryContext mcxt,
										   BTreeLocationHint *hint,
										   bool *deleted,
										   TupleFetchCallback cb,
										   void *arg);

extern OTuple o_find_tuple_version(BTreeDescr *desc, Page p,
								   BTreePageItemLocator *loc,
								   CommitSeqNo csn, CommitSeqNo *tupleCsn,
								   MemoryContext mcxt, TupleFetchCallback cb,
								   void *arg);

#endif							/* __BTREE_ITERATOR_H__ */
