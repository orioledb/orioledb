/*-------------------------------------------------------------------------
 *
 * iterator.h
 *		Declarations of orioledb B-tree iterator.
 *
 * Copyright (c) 2021-2026, Oriole DB Inc.
 * Copyright (c) 2025-2026, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/btree/iterator.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __BTREE_ITERATOR_H__
#define __BTREE_ITERATOR_H__

#include "btree.h"
#include "btree/page_contents.h"

/*
 * Return values for TupleFetchCallback used by o_find_tuple_version().
 *
 * The callback is invoked while traversing the undo chain: first for the
 * on-page tuple, then for each historical version from undo.  It selects
 * among versions that share the same snapshot csn/xlogptr (e.g. multiple uncommitted
 * versions within the same in-progress transaction).
 *
 * OTupleFetchMatch    - the tuple matches; stop and return it.
 * OTupleFetchNotMatch - no match found; stop and return NULL.
 * OTupleFetchNext     - skip this version; continue to the next undo record.
 */
typedef enum
{
	OTupleFetchNext,
	OTupleFetchMatch,
	OTupleFetchNotMatch
} TupleFetchCallbackResult;

typedef TupleFetchCallbackResult (*TupleFetchCallback) (OTuple tuple,
														OXid tupOxid,
														OSnapshot *oSnapshot,
														void *arg,
														bool oxidIsFinished);

extern OTuple o_btree_find_tuple_by_key(BTreeDescr *desc, void *key,
										BTreeKeyType kind,
										OSnapshot *read_o_snapshot,
										CommitSeqNo *out_csn,
										MemoryContext mcxt,
										BTreeLocationHint *hint);
extern OTuple o_btree_find_tuples_start(BTreeDescr *desc, void *key,
										BTreeKeyType kind,
										OSnapshot *read_o_snapshot,
										ScanDirection scanDir,
										CommitSeqNo *out_csn,
										MemoryContext mcxt,
										BTreeLocationHint *hint,
										bool *deleted,
										TupleFetchCallback cb,
										void *arg,
										BTreeIterator **out_it);
extern OTuple o_btree_find_tuples_continue(BTreeIterator *it,
										   void *key,
										   BTreeKeyType kind,
										   CommitSeqNo *out_csn,
										   BTreeLocationHint *hint,
										   bool *deleted);
extern void o_btree_find_tuples_finish(BTreeIterator *it);

extern BTreeIterator *o_btree_iterator_create(BTreeDescr *desc, void *key,
											  BTreeKeyType kind,
											  OSnapshot *o_snapshot,
											  ScanDirection scanDir);
extern void o_btree_iterator_advance(BTreeIterator *it,
									 void *key, BTreeKeyType kind);
extern void o_btree_iterator_set_tuple_ctx(BTreeIterator *it,
										   MemoryContext tupleCxt);
extern void o_btree_iterator_set_callback(BTreeIterator *it,
										  TupleFetchCallback callback,
										  void *arg);
extern void o_btree_iterator_set_interruptible(BTreeIterator *it);
extern OTuple o_btree_iterator_fetch(BTreeIterator *it,
									 CommitSeqNo *tuple_csn,
									 void *endKey, BTreeKeyType endKind,
									 bool endIsIncluded,
									 BTreeLocationHint *hint);

/*
 * Rows an iterator hands out in one go: `count` leaf items from `loc` on, all
 * in one chunk of the iterator's page image, every one visible and within the
 * end bound.  The image belongs to the iterator, so the rows must be read
 * before the iterator is called again.
 */
typedef struct BTreeIteratorBatch
{
	Page		img;
	BTreePageItemLocator loc;
	OffsetNumber count;
	CommitSeqNo csn;
	/* The end bound falls right after the batch: nothing follows it */
	bool		endReached;
} BTreeIteratorBatch;

extern bool o_btree_iterator_fetch_batch(BTreeIterator *it,
										 void *endKey, BTreeKeyType endKind,
										 bool endIsIncluded,
										 BTreeIteratorBatch *batch);

/* The batch's current row */
static inline OTuple
o_btree_iterator_batch_tuple(BTreeIteratorBatch *batch)
{
	OTuple		tuple;

	Assert(batch->count > 0);
	BTREE_PAGE_READ_LEAF_TUPLE(tuple, batch->img, &batch->loc);
	return tuple;
}

/* Step to the batch's next row */
static inline void
o_btree_iterator_batch_next(BTreeIteratorBatch *batch)
{
	Assert(batch->count > 0);
	batch->count--;
	batch->loc.itemOffset++;
}

extern OTuple btree_iterate_raw(BTreeIterator *it, void *end,
								BTreeKeyType endKind, bool endInclude,
								bool *scanEnd, BTreeLocationHint *hint);
extern OTuple btree_iterate_all(BTreeIterator *it, void *end,
								BTreeKeyType endKind, bool endInclude,
								bool *scanEnd, BTreeLocationHint *hint,
								BTreeLeafTuphdr **tupHdr);
extern void btree_iterator_free(BTreeIterator *it);
extern void btree_iterator_free_extended(BTreeIterator *it, bool fromResowner);

struct FastpathFindDownlinkMeta;
extern OTuple o_btree_find_tuple_by_key_meta(BTreeDescr *desc, void *key,
											 BTreeKeyType kind,
											 OSnapshot *read_o_snapshot,
											 CommitSeqNo *out_csn,
											 MemoryContext mcxt,
											 BTreeLocationHint *hint,
											 struct FastpathFindDownlinkMeta *meta);
extern OTuple o_btree_find_tuple_by_key_cb(BTreeDescr *desc, void *key,
										   BTreeKeyType kind,
										   OSnapshot *read_o_snapshot,
										   CommitSeqNo *out_csn,
										   MemoryContext mcxt,
										   BTreeLocationHint *hint,
										   bool *deleted,
										   TupleFetchCallback cb,
										   void *arg);

extern OTuple o_find_tuple_version(BTreeDescr *desc, Page p,
								   BTreePageItemLocator *loc,
								   OSnapshot *oSnapshot,
								   CommitSeqNo *tupleCsn,
								   MemoryContext mcxt,
								   TupleFetchCallback cb,
								   void *arg);

#endif							/* __BTREE_ITERATOR_H__ */
