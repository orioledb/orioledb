/*-------------------------------------------------------------------------
 *
 * undo.c
 *		Routines dealing with undo records of orioledb B-tree.
 *
 * Copyright (c) 2021-2022, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/btree/undo.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "orioledb.h"

#include "btree/find.h"
#include "btree/io.h"
#include "btree/merge.h"
#include "btree/page_chunks.h"
#include "btree/undo.h"
#include "recovery/recovery.h"
#include "tableam/descr.h"
#include "transam/oxid.h"
#include "transam/undo.h"
#include "utils/stopevent.h"
#include "utils/page_pool.h"

#include "access/transam.h"
#include "miscadmin.h"
#include "utils/inval.h"

/* Undo records */
typedef struct
{
	UndoStackItem header;
	BTreeOperationType action;
	ORelOids	oids;
	OInMemoryBlkno blkno;
	uint32		pageChangeCount;
	BTreeLeafTuphdr tuphdr;
} BTreeModifyUndoStackItem;

typedef struct
{
	OnCommitUndoStackItem header;
	Oid			datoid;
	Oid			relid;
	Oid			oldRelnode;
	int			oldNumTreeOids;
	Oid			newRelnode;
	int			newNumTreeOids;
	bool		fsync;
	ORelOids	oids[FLEXIBLE_ARRAY_MEMBER];
} RelnodeUndoStackItem;

static void clean_chain_has_locks_flag(UndoLocation location,
									   BTreeLeafTuphdr *pageTuphdr,
									   OInMemoryBlkno blkno);

/*
 * Add page image to the undo log.
 */
UndoLocation
page_add_item_to_undo(BTreeDescr *desc, Pointer p, CommitSeqNo imageCsn,
					  OTuple *splitKey, LocationIndex splitKeyLen)
{
	UndoPageImageHeader *header;
	UndoLocation undoLocation;
	Pointer		ptr;

	Assert(O_PAGE_IS(p, LEAF));

	Assert(desc->undoType != UndoReserveNone);
	if (splitKey)
		ptr = get_undo_record(desc->undoType, &undoLocation,
							  O_SPLIT_UNDO_IMAGE_SIZE(splitKeyLen));
	else
		ptr = get_undo_record(desc->undoType, &undoLocation,
							  O_COMPACT_UNDO_IMAGE_SIZE);

	header = (UndoPageImageHeader *) ptr;
	if (splitKey)
	{
		header->type = UndoPageImageSplit;
		header->splitKeyFlags = splitKey->formatFlags;
		header->splitKeyLen = splitKeyLen;
	}
	else
	{
		header->type = UndoPageImageCompact;
	}
	ptr += MAXALIGN(sizeof(UndoPageImageHeader));
	memcpy(ptr, p, ORIOLEDB_BLCKSZ);
	if (splitKey)
	{
		ptr += ORIOLEDB_BLCKSZ;
		memcpy(ptr, splitKey->data, splitKeyLen);
	}

	return undoLocation;
}

/*
 * Given page item modified by in-progress transaction.  Rollback changes
 * using undo chain.  Specify 'wholeChain' flag to revert all in-progress
 * changes from the chain.  Otherise, only last change item is reverted.
 *
 * Return true if page item still exists.
 *
 * 'nonLockTuphdrPtr' and 'nonLockUndoLocation' are a hint to the first
 * non-lock-only undo record in the chain.
 */
bool
page_item_rollback(BTreeDescr *desc, Page p, BTreePageItemLocator *locator,
				   bool wholeChain, BTreeLeafTuphdr *nonLockTuphdrPtr,
				   UndoLocation nonLockUndoLocation)
{
	Pointer		item;
	BTreeLeafTuphdr *tuphdr,
				nonLockTuphdr;

	item = BTREE_PAGE_LOCATOR_GET_ITEM(p, locator);
	tuphdr = (BTreeLeafTuphdr *) item;

	if (!nonLockTuphdrPtr)
	{
		nonLockTuphdr = *tuphdr;
		nonLockTuphdrPtr = &nonLockTuphdr;
		nonLockUndoLocation = find_non_lock_only_undo_record(nonLockTuphdrPtr);
	}

retry:

	Assert(O_PAGE_IS(p, LEAF));

	if (tuphdr->deleted)
	{
		OTuple		prev_tuple;

		/*
		 * Revert deletion.  Assuming tuple is deleted, we shouldn't have any
		 * row-level lock on this tuple.
		 */
		Assert(!UndoLocationIsValid(nonLockUndoLocation));
		Assert(UndoLocationIsValid(tuphdr->undoLocation));
		Assert(UNDO_REC_EXISTS(tuphdr->undoLocation));

		get_prev_leaf_header_from_undo(tuphdr, true);
		BTREE_PAGE_READ_TUPLE(prev_tuple, p, locator);
		PAGE_SUB_N_VACATED(p,
						   BTreeLeafTuphdrSize +
						   MAXALIGN(o_btree_len(desc, prev_tuple, OTupleLength)));
		tuphdr->formatFlags = 0;

		if (!UndoLocationIsValid(nonLockUndoLocation))
			*nonLockTuphdrPtr = *tuphdr;

		if (!XACT_INFO_IS_FINISHED(tuphdr->xactInfo) && wholeChain)
			goto retry;
	}
	else if (UndoLocationIsValid(nonLockTuphdrPtr->undoLocation))
	{
		/*
		 * Current tuple is not deleted.  And there is a pointer to previous
		 * version in chain.  This must be update (or insert to previously
		 * deleted tuple).
		 */
		OTuple		tuple;
		int			prev_tuplen,
					tuplen,
					itemlen;
		BTreeLeafTuphdr prev_header;

		prev_header = *nonLockTuphdrPtr;
		tuple.formatFlags = BTREE_PAGE_GET_ITEM_FLAGS(p, locator);
		tuple.data = item + BTreeLeafTuphdrSize;
		prev_tuplen = o_btree_len(desc, tuple, OTupleLength);

		tuplen = BTREE_PAGE_GET_ITEM_SIZE(p, locator) - BTreeLeafTuphdrSize;
		get_prev_leaf_header_and_tuple_from_undo(&prev_header,
												 &tuple,
												 tuplen);
		tuplen = o_btree_len(desc, tuple, OTupleLength);
		itemlen = BTreeLeafTuphdrSize + MAXALIGN(tuplen);

		Assert(itemlen <= BTREE_PAGE_GET_ITEM_SIZE(p, locator));
		if (XACT_INFO_IS_FINISHED(prev_header.xactInfo))
		{
			PAGE_SUB_N_VACATED(p, BTREE_PAGE_GET_ITEM_SIZE(p, locator) -
							   (BTreeLeafTuphdrSize + MAXALIGN(prev_tuplen)));
			page_locator_resize_item(p, locator, itemlen);
		}
		else
		{
			PAGE_ADD_N_VACATED(p, MAXALIGN(prev_tuplen));
			PAGE_SUB_N_VACATED(p, MAXALIGN(tuplen));
		}
		if (prev_header.deleted)
			PAGE_ADD_N_VACATED(p, BTreeLeafTuphdrSize + MAXALIGN(tuplen));

		if (!UndoLocationIsValid(nonLockUndoLocation))
		{
			*nonLockTuphdrPtr = *tuphdr = prev_header;
		}
		else
		{
			tuphdr->deleted = prev_header.deleted;
			nonLockTuphdrPtr->undoLocation = prev_header.undoLocation;
			nonLockTuphdrPtr->xactInfo = prev_header.xactInfo;
			update_leaf_header_in_undo(nonLockTuphdrPtr, nonLockUndoLocation);
		}

		BTREE_PAGE_SET_ITEM_FLAGS(p, locator, tuple.formatFlags);

		/* Follow the row-level undo chain if needed */
		if ((UndoLocationIsValid(nonLockUndoLocation) ||
			 !XACT_INFO_IS_FINISHED(prev_header.xactInfo)) && wholeChain)
		{
			/* Find the next item in the chain */
			nonLockUndoLocation = find_non_lock_only_undo_record(nonLockTuphdrPtr);
			if (XACT_INFO_IS_FINISHED(nonLockTuphdrPtr->xactInfo))
				return true;
			item = BTREE_PAGE_LOCATOR_GET_ITEM(p, locator);
			tuphdr = (BTreeLeafTuphdr *) item;
			goto retry;
		}
	}
	else
	{
		OTuple		prev_tuple;

		/*
		 * Revert insertion of new tuple.  Assuming insertion is in-progress,
		 * we shouldn't have any row-level lock on this tuple.
		 */
		Assert(!UndoLocationIsValid(nonLockUndoLocation));

		BTREE_PAGE_READ_TUPLE(prev_tuple, p, locator);
		PAGE_SUB_N_VACATED(p, BTREE_PAGE_GET_ITEM_SIZE(p, locator) -
						   (BTreeLeafTuphdrSize + MAXALIGN(o_btree_len(desc, prev_tuple, OTupleLength))));

		page_locator_delete_item(p, locator);
		return false;
	}
	return true;
}

static Jsonb *
undo_record_key_stopevent_params(BTreeOperationType action,
								 BTreeDescr *desc,
								 OTuple tuple, OXid oxid)
{
	JsonbParseState *state = NULL;
	Jsonb	   *res;
	MemoryContext mctx = MemoryContextSwitchTo(stopevents_cxt);

	pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
	if (action == BTreeOperationInsert)
		jsonb_push_string_key(&state, "action", "insert");
	else if (action == BTreeOperationUpdate)
		jsonb_push_string_key(&state, "action", "update");
	else if (action == BTreeOperationDelete)
		jsonb_push_string_key(&state, "action", "delete");
	else if (action == BTreeOperationLock)
		jsonb_push_string_key(&state, "action", "lock");
	jsonb_push_int8_key(&state, "oxid", oxid);
	btree_desc_stopevent_params_internal(desc, &state);
	jsonb_push_key(&state, "key");
	if (action == BTreeOperationUpdate)
	{
		OTuple		key;
		bool		allocated;

		key = o_btree_tuple_make_key(desc, tuple, NULL, true, &allocated);
		(void) o_btree_key_to_jsonb(desc, key, &state);
		if (allocated)
			pfree(key.data);
	}
	else
	{
		(void) o_btree_key_to_jsonb(desc, tuple, &state);
	}
	res = JsonbValueToJsonb(pushJsonbValue(&state, WJB_END_OBJECT, NULL));
	MemoryContextSwitchTo(mctx);

	return res;
}

/*
 * Make undo record associated with give tuple and operation.
 */
BTreeLeafTuphdr *
make_undo_record(BTreeDescr *desc, OTuple tuple, bool is_tuple,
				 BTreeOperationType action, OInMemoryBlkno blkno,
				 uint32 pageChangeCount,
				 UndoLocation *undoLocation)
{
	LocationIndex tuplelen;
	BTreeModifyUndoStackItem *item;
	LocationIndex size;

	if (action == BTreeOperationUpdate)
	{
		Assert(is_tuple);
		tuplelen = o_btree_len(desc, tuple, OTupleLength);
	}
	else
	{
		tuplelen = o_btree_len(desc, tuple, is_tuple ? OTupleKeyLength : OKeyLength);
	}

	size = sizeof(BTreeModifyUndoStackItem) + tuplelen;
	item = (BTreeModifyUndoStackItem *) get_undo_record(desc->undoType,
														undoLocation,
														MAXALIGN(size));
	item->header.itemSize = size;
	if (action == BTreeOperationLock)
		item->header.type = RowLockUndoItemType;
	else
		item->header.type = ModifyUndoItemType;
	item->header.indexType = desc->type;
	item->action = action;
	item->blkno = blkno;
	item->pageChangeCount = pageChangeCount;
	item->oids = desc->oids;

	if (action == BTreeOperationUpdate || !is_tuple)
	{
		memcpy((Pointer) item + sizeof(BTreeModifyUndoStackItem),
			   tuple.data,
			   tuplelen);
		item->tuphdr.formatFlags = tuple.formatFlags;
	}
	else
	{
		bool		key_palloc = false;
		OTuple		key;

		memset((Pointer) item + sizeof(BTreeModifyUndoStackItem), 0, tuplelen);
		key = o_btree_tuple_make_key(desc, tuple,
									 (Pointer) item + sizeof(BTreeModifyUndoStackItem),
									 true, &key_palloc);
		item->tuphdr.formatFlags = key.formatFlags;
		Assert(!key_palloc);
	}

	add_new_undo_stack_item(*undoLocation);

	*undoLocation += offsetof(BTreeModifyUndoStackItem, tuphdr);
	return &item->tuphdr;
}

static BTreeDescr *
get_tree_descr(ORelOids oids, OIndexType type)
{
	if (IS_SYS_TREE_OIDS(oids))
	{
		return get_sys_tree(oids.relnode);
	}
	else
	{
		OIndexDescr *descr = o_fetch_index_descr(oids, type, false, NULL);

		if (!descr)
			return NULL;
		return &descr->desc;
	}
}

/*
 * Callback for aborting B-tree record modification.
 */
void
modify_undo_callback(UndoLocation location, UndoStackItem *baseItem,
					 OXid oxid, bool abort, bool changeCountsValid)
{
	BTreeModifyUndoStackItem *item = (BTreeModifyUndoStackItem *) baseItem;
	BTreeDescr *desc = get_tree_descr(item->oids, item->header.indexType);
	OTuple		tuple;
	Page		p;
	int			cmp;
	OInMemoryBlkno blkno;
	BTreePageItemLocator *loc;
	BTreeLeafTuphdr *tupHdr,
				nonLockTupHdr;
	UndoLocation nonLockUndoLocation;
	OBTreeFindPageContext context;
	BTreeKeyType keyType = item->action == BTreeOperationUpdate ? BTreeKeyLeafTuple : BTreeKeyNonLeafKey;

	Assert(abort);

	if (!desc)
		return;

	tuple.formatFlags = item->tuphdr.formatFlags;
	tuple.data = (Pointer) item + sizeof(BTreeModifyUndoStackItem);

	if (STOPEVENTS_ENABLED())
	{
		Jsonb	   *params = undo_record_key_stopevent_params(item->action,
															  desc,
															  tuple, oxid);

		STOPEVENT(STOPEVENT_APPLY_UNDO, params);
	}

	o_btree_load_shmem(desc);
	init_page_find_context(&context, desc,
						   COMMITSEQNO_INPROGRESS,
						   BTREE_PAGE_FIND_MODIFY);

	if (!changeCountsValid)
		item->pageChangeCount = InvalidOPageChangeCount;

	if (!refind_page(&context, (Pointer) &tuple, keyType, 0,
					 item->blkno, item->pageChangeCount))
	{
		/*
		 * BTree can be already deleted and cleaned by
		 * btree_relnode_undo_callback().
		 */
		return;
	}

	blkno = context.items[context.index].blkno;
	p = O_GET_IN_MEMORY_PAGE(blkno);
	loc = &context.items[context.index].locator;

	if (BTREE_PAGE_LOCATOR_IS_VALID(p, loc))
	{
		OTuple		leafTup;

		BTREE_PAGE_READ_LEAF_ITEM(tupHdr, leafTup, p, loc);
		cmp = o_btree_cmp(desc, &tuple, keyType, &leafTup, BTreeKeyLeafTuple);
	}
	else
		cmp = 1;

	if (cmp != 0)
	{
		/*
		 * We can't find the required key.  This might happend if operation
		 * was already "undone" earlier.
		 */
		unlock_page(blkno);
		return;
	}

	nonLockTupHdr = *tupHdr;
	nonLockUndoLocation = find_non_lock_only_undo_record(&nonLockTupHdr);

	if (!XACT_INFO_OXID_EQ(nonLockTupHdr.xactInfo, oxid))
	{
		/*
		 * The key is found, but it doesn't belong to our transaction.  Again,
		 * this might happend if operation was already "undone" earlier.
		 */
		unlock_page(blkno);
		return;
	}

	page_block_reads(blkno);

	/*
	 * Check that undo chain item matches to the tuple item.
	 */
	if (nonLockTupHdr.undoLocation == location + offsetof(BTreeModifyUndoStackItem, tuphdr) ||
		(!UndoLocationIsValid(nonLockTupHdr.undoLocation) && item->action == BTreeOperationInsert))
	{
		(void) page_item_rollback(desc, p, loc, false,
								  &nonLockTupHdr, nonLockUndoLocation);
	}

	MARK_DIRTY(desc->ppool, blkno);
	if (blkno != desc->rootInfo.rootPageBlkno && is_page_too_sparse(desc, p))
	{
		/* We can try to merge this page */
		btree_try_merge_and_unlock(context.desc, blkno, true, true);
	}
	else
		unlock_page(blkno);
}

/*
 * Callback for aborting B-tree tuple lock.
 */
void
lock_undo_callback(UndoLocation location, UndoStackItem *baseItem, OXid oxid,
				   bool abort, bool changeCountsValid)
{
	BTreeModifyUndoStackItem *item = (BTreeModifyUndoStackItem *) baseItem;
	BTreeDescr *desc = get_tree_descr(item->oids, item->header.indexType);
	OTuple		key;
	Page		p;
	int			cmp;
	OInMemoryBlkno blkno;
	BTreeLeafTuphdr *page_tuphdr,
				tuphdr;
	BTreePageItemLocator *locptr;
	OBTreeFindPageContext context;
	UndoLocation tuphdrUndoLocation,
				lastLockOnlyUndoLocation = InvalidUndoLocation;

	Assert(abort);

	if (!desc)
		return;

	key.formatFlags = item->tuphdr.formatFlags;
	key.data = (Pointer) item + sizeof(BTreeModifyUndoStackItem);

	if (STOPEVENTS_ENABLED())
	{
		Jsonb	   *params = undo_record_key_stopevent_params(BTreeOperationLock,
															  desc, key, oxid);

		STOPEVENT(STOPEVENT_APPLY_UNDO, params);
	}

	o_btree_load_shmem(desc);
	init_page_find_context(&context, desc, COMMITSEQNO_INPROGRESS, BTREE_PAGE_FIND_MODIFY);
	if (!changeCountsValid)
		item->pageChangeCount = InvalidOPageChangeCount;

	if (!refind_page(&context, (Pointer) &key, BTreeKeyNonLeafKey, 0, item->blkno, item->pageChangeCount))
	{
		/*
		 * BTree can be already deleted and cleaned by
		 * btree_relnode_undo_callback().
		 */
		return;
	}

	blkno = context.items[context.index].blkno;
	p = O_GET_IN_MEMORY_PAGE(blkno);
	locptr = &context.items[context.index].locator;

	if (BTREE_PAGE_LOCATOR_GET_OFFSET(p, locptr) < BTREE_PAGE_ITEMS_COUNT(p))
	{
		OTuple		leafTup;

		BTREE_PAGE_READ_TUPLE(leafTup, p, locptr);
		cmp = o_btree_cmp(desc, &key, BTreeKeyNonLeafKey, &leafTup, BTreeKeyLeafTuple);
	}
	else
		cmp = 1;

	if (cmp != 0)
	{
		/* Row already gone. Nothing to do. */
		unlock_page(blkno);
		return;
	}

	page_tuphdr = (BTreeLeafTuphdr *) BTREE_PAGE_LOCATOR_GET_ITEM(p, locptr);
	tuphdr = *page_tuphdr;
	tuphdrUndoLocation = InvalidUndoLocation;

	while (!XACT_INFO_IS_FINISHED(tuphdr.xactInfo) || tuphdr.chainHasLocks)
	{
		bool		delete_record = false;
		UndoLocation undoLocation = tuphdr.undoLocation;
		BTreeLeafTuphdr prev_tuphdr = tuphdr;

		Assert(UndoLocationIsValid(undoLocation) && UNDO_REC_EXISTS(undoLocation));
		get_prev_leaf_header_from_undo(&prev_tuphdr, false);

		if (XACT_INFO_IS_LOCK_ONLY(tuphdr.xactInfo) && XACT_INFO_GET_OXID(tuphdr.xactInfo) == oxid)
		{
			if (tuphdr.undoLocation == location + offsetof(BTreeModifyUndoStackItem, tuphdr))
				delete_record = true;
		}

		if (delete_record)
		{
			if (!tuphdr.chainHasLocks &&
				XACT_INFO_IS_LOCK_ONLY(tuphdr.xactInfo))
				clean_chain_has_locks_flag(lastLockOnlyUndoLocation,
										   page_tuphdr, blkno);

			if (!UndoLocationIsValid(tuphdrUndoLocation))
			{
				page_block_reads(blkno);
				page_tuphdr->xactInfo = prev_tuphdr.xactInfo;
				page_tuphdr->undoLocation = prev_tuphdr.undoLocation;
				page_tuphdr->chainHasLocks = prev_tuphdr.chainHasLocks;
				tuphdr = *page_tuphdr;
				MARK_DIRTY(desc->ppool, blkno);
			}
			else
			{
				tuphdr.xactInfo = prev_tuphdr.xactInfo;
				tuphdr.undoLocation = prev_tuphdr.undoLocation;
				tuphdr.chainHasLocks = prev_tuphdr.chainHasLocks;
				update_leaf_header_in_undo(&tuphdr, tuphdrUndoLocation);
			}
		}

		/*
		 * We should be able to find at CSN-record or invalid undo location
		 * before running out of undo records.
		 */
		Assert(UNDO_REC_EXISTS(undoLocation));

		if (XACT_INFO_IS_LOCK_ONLY(tuphdr.xactInfo))
			lastLockOnlyUndoLocation = tuphdrUndoLocation;

		tuphdr = prev_tuphdr;
		tuphdrUndoLocation = undoLocation;
		undoLocation = tuphdr.undoLocation;
	}
	unlock_page(blkno);
}

/*
 * Change relnode of btree.
 */
void
btree_relnode_undo_callback(UndoLocation location, UndoStackItem *baseItem,
							OXid oxid, bool abort, bool changeCountsValid)
{
	RelnodeUndoStackItem *relnode_item = (RelnodeUndoStackItem *) baseItem;
	Oid			datoid,
				reloid,
				dropRelnode,
				remainRelnode;
	int			dropNumTreeOids;
	ORelOids   *dropTreeOids;

	datoid = relnode_item->datoid;
	reloid = relnode_item->relid;
	if (!abort)
	{
		dropRelnode = relnode_item->oldRelnode;
		remainRelnode = relnode_item->newRelnode;
		dropTreeOids = &relnode_item->oids[0];
		dropNumTreeOids = relnode_item->oldNumTreeOids;
	}
	else
	{
		dropRelnode = relnode_item->newRelnode;
		remainRelnode = relnode_item->oldRelnode;
		dropTreeOids = &relnode_item->oids[relnode_item->oldNumTreeOids];
		dropNumTreeOids = relnode_item->newNumTreeOids;
	}

	/* Fsync new files if required */
	if (!abort &&
		OidIsValid(relnode_item->newRelnode) &&
		relnode_item->fsync)
	{
		int			numTreeOids = relnode_item->newNumTreeOids;
		ORelOids   *treeOids = &relnode_item->oids[relnode_item->oldNumTreeOids];
		int			i;

		for (i = 0; i < numTreeOids; i++)
			fsync_btree_files(treeOids[i].datoid,
							  treeOids[i].relnode);
	}

	if (OidIsValid(dropRelnode))
	{
		bool		recovery = is_recovery_in_progress();
		ORelOids	oids = {datoid, reloid, dropRelnode};
		int			i;

		if (!recovery)
			o_tables_rel_lock_extended_no_inval(&oids, AccessExclusiveLock, false);
		o_tables_rel_lock_extended_no_inval(&oids, AccessExclusiveLock, true);
		CacheInvalidateRelcacheByDbidRelid(datoid, reloid);
		o_invalidate_oids(oids);
		if (!recovery)
			o_tables_rel_unlock_extended(&oids, AccessExclusiveLock, false);
		o_tables_rel_unlock_extended(&oids, AccessExclusiveLock, true);

		for (i = 0; i < dropNumTreeOids; i++)
		{
			if (!recovery)
				o_tables_rel_lock_extended_no_inval(&dropTreeOids[i], AccessExclusiveLock, false);
			o_tables_rel_lock_extended_no_inval(&dropTreeOids[i], AccessExclusiveLock, true);
			cleanup_btree(dropTreeOids[i].datoid, dropTreeOids[i].relnode);
			o_invalidate_oids(dropTreeOids[i]);
			if (!recovery)
				o_tables_rel_unlock_extended(&dropTreeOids[i], AccessExclusiveLock, false);
			o_tables_rel_unlock_extended(&dropTreeOids[i], AccessExclusiveLock, true);
		}
	}

	if (OidIsValid(remainRelnode))
	{
		ORelOids	oids = {datoid, reloid, remainRelnode};

		o_invalidate_oids(oids);
	}
}

/*
 * oldTreeOids and newTreeOids should be allocated in CurTransactionContext.
 */
static inline void
add_undo_relnode(ORelOids oldOids, ORelOids *oldTreeOids, int oldNumTreeOids,
				 ORelOids newOids, ORelOids *newTreeOids, int newNumTreeOids,
				 bool fsync)
{
	LocationIndex size;
	UndoLocation location;
	RelnodeUndoStackItem *item;

	size = offsetof(RelnodeUndoStackItem, oids) + sizeof(ORelOids) * (oldNumTreeOids + newNumTreeOids);
	item = (RelnodeUndoStackItem *) get_undo_record_unreserved(UndoReserveTxn, &location, MAXALIGN(size));

	item->header.base.type = RelnodeUndoItemType;
	item->header.base.itemSize = size;
	item->header.base.indexType = oIndexPrimary;
	Assert(ORelOidsIsValid(oldOids) || ORelOidsIsValid(newOids));
	if (ORelOidsIsValid(oldOids))
	{
		item->datoid = oldOids.datoid;
		item->relid = oldOids.reloid;
	}
	else
	{
		item->datoid = newOids.datoid;
		item->relid = newOids.reloid;
	}
	item->oldRelnode = oldOids.relnode;
	item->oldNumTreeOids = oldNumTreeOids;
	item->newRelnode = newOids.relnode;
	item->newNumTreeOids = newNumTreeOids;
	item->fsync = fsync;

	if (oldNumTreeOids > 0)
	{
		Assert(oldTreeOids);
		memcpy(item->oids,
			   oldTreeOids,
			   sizeof(ORelOids) * oldNumTreeOids);
	}
	if (newNumTreeOids > 0)
	{
		Assert(newTreeOids);
		memcpy(&item->oids[oldNumTreeOids],
			   newTreeOids,
			   sizeof(ORelOids) * newNumTreeOids);
	}

	/*
	 * This might happend before we accessed oxid.  So, ensure we've assigned
	 * it.
	 */
	(void) get_current_oxid();

	oxid_needs_wal_flush = true;
	add_new_undo_stack_item(location);

	release_undo_size(UndoReserveTxn);
}

void
add_undo_truncate_relnode(ORelOids oldOids, ORelOids *oldTreeOids,
						  int oldNumTreeOids,
						  ORelOids newOids, ORelOids *newTreeOids,
						  int newNumTreeOids)
{
	Assert(ORelOidsIsValid(oldOids) && ORelOidsIsValid(newOids));
	Assert(oldOids.datoid == newOids.datoid);
	Assert(oldOids.reloid == newOids.reloid);

	add_undo_relnode(oldOids, oldTreeOids, oldNumTreeOids,
					 newOids, newTreeOids, newNumTreeOids, false);
}

void
add_undo_drop_relnode(ORelOids oids, ORelOids *treeOids, int numTreeOids)
{
	ORelOids	invalid = {InvalidOid, InvalidOid, InvalidOid};

	Assert(ORelOidsIsValid(oids));
	add_undo_relnode(oids, treeOids, numTreeOids, invalid, NULL, 0, false);
}

void
add_undo_create_relnode(ORelOids oids, ORelOids *treeOids, int numTreeOids)
{
	ORelOids	invalid = {InvalidOid, InvalidOid, InvalidOid};

	Assert(ORelOidsIsValid(oids));
	add_undo_relnode(invalid, NULL, 0, oids, treeOids, numTreeOids, true);
}

static void
read_hikey_from_undo(UndoLocation location, Page dest, LocationIndex *loc)
{
	undo_read(location, sizeof(BTreePageHeader), dest);
	*loc = sizeof(BTreePageHeader);
	undo_read(location + *loc,
			  ((BTreePageHeader *) dest)->hikeysEnd - *loc,
			  dest + *loc);
	*loc = ((BTreePageHeader *) dest)->hikeysEnd;
}

/*
 * Finds page image in undoLocation.
 */
void
get_page_from_undo(BTreeDescr *desc, UndoLocation undoLocation, Pointer key,
				   BTreeKeyType kind, Pointer dest,
				   bool *is_left, bool *is_right, OFixedKey *lokey,
				   OFixedKey *page_lokey, OTuple *page_hikey)
{
	UndoPageImageHeader header;
	int			cmp,
				cmp_expected;
	OTuple		hikey;
	UndoLocation left_loc,
				right_loc;
	LocationIndex loc = 0;

	undo_read(undoLocation, sizeof(UndoPageImageHeader), (Pointer) &header);
	left_loc = undoLocation + MAXALIGN(sizeof(UndoPageImageHeader));

	if (is_left != NULL)
		*is_left = false;

	if (is_right != NULL)
		*is_right = false;

	/* there is only one page, no need to choose */
	if (header.type == UndoPageImageSplit ||
		header.type == UndoPageImageCompact)
	{
		if (is_left != NULL)
			*is_left = true;
		if (is_right != NULL)
			*is_right = true;
		undo_read(left_loc, ORIOLEDB_BLCKSZ, dest);
		if (page_lokey && header.type == UndoPageImageSplit)
		{
			bool		set_page_lokey = false;

			if (!page_hikey || O_TUPLE_IS_NULL(*page_hikey))
			{
				set_page_lokey = true;
			}
			else if (!O_PAGE_IS(dest, RIGHTMOST))
			{
				OTuple		hikey;
				int			cmp;

				BTREE_PAGE_GET_HIKEY(hikey, dest);
				cmp = o_btree_cmp(desc, page_hikey, BTreeKeyNonLeafKey, &hikey, BTreeKeyNonLeafKey);
				Assert(cmp <= 0);
				if (cmp == 0)
					set_page_lokey = true;
			}

			if (set_page_lokey)
			{
				undo_read(left_loc + ORIOLEDB_BLCKSZ,
						  header.splitKeyLen,
						  page_lokey->fixedData);
				page_lokey->tuple.formatFlags = header.splitKeyFlags;
				page_lokey->tuple.data = (Pointer) &page_lokey->fixedData;
			}
		}
		return;
	}

	right_loc = left_loc + ORIOLEDB_BLCKSZ;

	/*
	 * It's dual undo log page image. We should make decision which page (left
	 * or right) should be returned.
	 */
	Assert(header.type == UndoPageImageMerge);
	switch (kind)
	{
		case BTreeKeyNone:
			if (is_left != NULL)
				*is_left = true;
			undo_read(left_loc, ORIOLEDB_BLCKSZ, dest);
			break;
		case BTreeKeyRightmost:
			if (is_right != NULL)
				*is_right = true;
			if (lokey != NULL)
			{
				read_hikey_from_undo(left_loc, dest, &loc);
				copy_fixed_hikey(desc, lokey, dest);
			}
			undo_read(right_loc, ORIOLEDB_BLCKSZ, dest);
			break;
		case BTreeKeyLeafTuple:
		case BTreeKeyNonLeafKey:
		case BTreeKeyBound:
		case BTreeKeyPageHiKey:
			Assert(key != NULL);

			read_hikey_from_undo(left_loc, dest, &loc);

			cmp_expected = kind == BTreeKeyPageHiKey ? 1 : 0;
			kind = kind == BTreeKeyPageHiKey ? BTreeKeyNonLeafKey : kind;
			BTREE_PAGE_GET_HIKEY(hikey, dest);

			cmp = o_btree_cmp(desc, key, kind, &hikey, BTreeKeyNonLeafKey);

			if (cmp >= cmp_expected)
			{
				if (is_right != NULL)
					*is_right = true;
				if (lokey != NULL)
					copy_fixed_hikey(desc, lokey, dest);
				undo_read(right_loc, ORIOLEDB_BLCKSZ, dest);
			}
			else
			{
				if (is_left != NULL)
					*is_left = true;
				undo_read(left_loc + loc, ORIOLEDB_BLCKSZ - loc, dest + loc);
			}
			break;
		default:
			Assert(false);
	}
}

/*
 * Copy images of the left and the right pages into undo log.
 */
UndoLocation
make_merge_undo_image(BTreeDescr *desc, Pointer left,
					  Pointer right, CommitSeqNo imageCsn)
{
	UndoPageImageHeader *header;
	UndoLocation undoLocation;
	Pointer		undo_rec;

	Assert(O_PAGE_IS(left, LEAF) && O_PAGE_IS(right, LEAF));

	Assert(desc->undoType != UndoReserveNone);
	undo_rec = get_undo_record(desc->undoType, &undoLocation, O_MERGE_UNDO_IMAGE_SIZE);

	header = (UndoPageImageHeader *) undo_rec;
	header->type = UndoPageImageMerge;
	undo_rec = undo_rec + MAXALIGN(sizeof(UndoPageImageHeader));

	memcpy(undo_rec, left, ORIOLEDB_BLCKSZ);
	memcpy(undo_rec + ORIOLEDB_BLCKSZ, right, ORIOLEDB_BLCKSZ);

	return undoLocation;
}

/*
 * Clean `chainHasLocks` flag on given and previous undo locations.
 */
static void
clean_chain_has_locks_flag(UndoLocation location, BTreeLeafTuphdr *pageTuphdr,
						   OInMemoryBlkno blkno)
{
	BTreeLeafTuphdr tuphdr;
	UndoLocation retainedUndoLocation;

	if (!is_recovery_process())
		retainedUndoLocation = get_snapshot_retained_undo_location();
	else
		retainedUndoLocation = pg_atomic_read_u64(&undo_meta->checkpointRetainStartLocation);

	/*
	 * Invalid location means that we should update starting from the
	 * pageTuphdr. Clean `chainHasLocks` flag there if needed.
	 */
	if (!UndoLocationIsValid(location) || location < retainedUndoLocation)
	{
		if (!pageTuphdr->chainHasLocks)
			return;

		page_block_reads(blkno);

		pageTuphdr->chainHasLocks = false;
		location = pageTuphdr->undoLocation;
	}

	/*
	 * Iteratively clean `chainHasLocks` flag in the rest of chain.
	 */
	while (UndoLocationIsValid(location) && location >= retainedUndoLocation)
	{
		Assert(UNDO_REC_EXISTS(location));

		undo_read(location, sizeof(tuphdr), (Pointer) &tuphdr);

		if (!tuphdr.chainHasLocks)
			break;

		tuphdr.chainHasLocks = false;
		undo_write(location, sizeof(tuphdr), (Pointer) &tuphdr);

		location = tuphdr.undoLocation;
	}
}


/*
 * Check for row-level lock conflict
 *
 * Returns true if lock conflict.  On lock conflict places the conflicting undo
 * record info *conflictTuphdr.
 *
 * Otherwise, places the first csn undo record info *conflictTuphdr.
 * If there is no such undo records, then *conflictTuphdr is set to
 * *pageTuphdr.
 *
 * Lock-only undo records from committed and aborted transactions are removed.
 * Own lock-only undo records of the same or weaker level are removed.
 */
bool
row_lock_conflicts(BTreeLeafTuphdr *pageTuphdr,
				   BTreeLeafTuphdr *conflictTuphdr,
				   UndoLocation *conflictUndoLocation,
				   RowLockMode mode, OXid my_oxid, CommitSeqNo my_csn,
				   OInMemoryBlkno blkno, UndoLocation savepointUndoLocation,
				   bool *redundant_row_locks, BTreeModifyLockStatus *lock_status)
{
	OTupleXactInfo xactInfo;
	bool		xactIsFinished;
	bool		xactIsFinal;
	RowLockMode	xactMode;
	UndoLocation undoLocation;
	UndoLocation lastLockOnlyUndoLocation;
	BTreeLeafTuphdr curTuphdr,
				finalTuphdr;
	UndoLocation curUndoLocation,
				finalUndoLocation;
	UndoLocation retainedUndoLocation = get_snapshot_retained_undo_location();
	bool		foundFinal;
	bool		result = false;

	finalTuphdr = curTuphdr = *pageTuphdr;
	finalUndoLocation = curUndoLocation = InvalidUndoLocation;
	lastLockOnlyUndoLocation = InvalidUndoLocation;
	xactInfo = curTuphdr.xactInfo;
	xactMode = XACT_INFO_GET_LOCK_MODE(xactInfo);
	if (ROW_LOCKS_CONFLICT(xactMode, mode))
	{
		xactIsFinal = xactIsFinished = XACT_INFO_IS_FINISHED(xactInfo);
	}
	else
	{
		CommitSeqNo		csn = XACT_INFO_MAP_CSN(xactInfo);

		xactIsFinished = !COMMITSEQNO_IS_INPROGRESS(csn);
		xactIsFinal = (csn < my_csn);
	}
	foundFinal = xactIsFinal;
	undoLocation = curTuphdr.undoLocation;

	while (curTuphdr.chainHasLocks ||
		   XACT_INFO_IS_LOCK_ONLY(xactInfo) ||
		   !xactIsFinal)
	{
		bool		prevChainHasLocks = false;
		bool		delete_record = false;

		if (XACT_INFO_IS_LOCK_ONLY(xactInfo) &&
			XACT_INFO_GET_OXID(xactInfo) == my_oxid)
		{
			/* Check if there are redundant row-level locks */
			if (xactMode <= mode &&
				(!UndoLocationIsValid(savepointUndoLocation) ||
				 (UndoLocationIsValid(undoLocation) &&
				  undoLocation >= savepointUndoLocation)))
				*redundant_row_locks = true;
			if (xactMode >= mode)
				*lock_status = Max(*lock_status, BTreeModifySameOrStrongerLock);
			else
				*lock_status = Max(*lock_status, BTreeModifyWeakerLock);
		}

		if (XACT_INFO_IS_LOCK_ONLY(xactInfo) &&
			ROW_LOCKS_CONFLICT(xactMode, mode))
		{
			BTreeLeafTuphdr prev_tuphdr;
			OXid		oxid = XACT_INFO_GET_OXID(xactInfo);

			Assert(UndoLocationIsValid(undoLocation));

			if (oxid != my_oxid)
			{
				CommitSeqNo csn;

				/*
				 * Row-level locks make sense only for in-progress
				 * transactions. We delete RLL for both committed and aborted
				 * transactions.
				 */
				csn = oxid_get_csn(oxid);
				if (COMMITSEQNO_IS_ABORTED(csn) ||
					COMMITSEQNO_IS_NORMAL(csn) ||
					COMMITSEQNO_IS_FROZEN(csn))
				{
					delete_record = true;
				}
				else if (!result || XACT_INFO_GET_OXID(conflictTuphdr->xactInfo) == my_oxid)
				{
					*conflictTuphdr = curTuphdr;
					*conflictUndoLocation = curUndoLocation;
					result = true;
				}
			}

			if (delete_record && undoLocation >= retainedUndoLocation)
			{
				Assert(UNDO_REC_EXISTS(undoLocation));

				prev_tuphdr = curTuphdr;
				get_prev_leaf_header_from_undo(&prev_tuphdr, false);
				if (!UndoLocationIsValid(curUndoLocation))
				{
					page_block_reads(blkno);
					pageTuphdr->xactInfo = prev_tuphdr.xactInfo;
					pageTuphdr->undoLocation = prev_tuphdr.undoLocation;
					pageTuphdr->chainHasLocks = prev_tuphdr.chainHasLocks;
				}
				else
				{
					/*
					 * Update chainHasLocks flag of the next undo records if
					 * needed.
					 */
					if (XACT_INFO_IS_LOCK_ONLY(curTuphdr.xactInfo) &&
						!curTuphdr.chainHasLocks)
					{
						clean_chain_has_locks_flag(lastLockOnlyUndoLocation,
												   pageTuphdr,
												   blkno);
						lastLockOnlyUndoLocation = InvalidUndoLocation;
					}

					curTuphdr.xactInfo = prev_tuphdr.xactInfo;
					curTuphdr.undoLocation = prev_tuphdr.undoLocation;
					curTuphdr.chainHasLocks = prev_tuphdr.chainHasLocks;
					update_leaf_header_in_undo(&curTuphdr,
											   curUndoLocation);

				}
			}
		}
		else if (!xactIsFinished)
		{
			if (XACT_INFO_GET_OXID(xactInfo) == my_oxid)
			{
				if (xactMode >= mode)
					*lock_status = Max(*lock_status, BTreeModifySameOrStrongerLock);
				else
					*lock_status = Max(*lock_status, BTreeModifyWeakerLock);
			}
			if (ROW_LOCKS_CONFLICT(xactMode, mode) &&
				(!result || (XACT_INFO_GET_OXID(conflictTuphdr->xactInfo) == my_oxid &&
				 XACT_INFO_GET_OXID(xactInfo) != my_oxid)))
			{
				*conflictTuphdr = curTuphdr;
				*conflictUndoLocation = curUndoLocation;
				result = true;
			}
		}

		if (!UndoLocationIsValid(undoLocation) ||
			undoLocation < retainedUndoLocation)
		{
			/*
			 * We have reached the end of "in-progress" undo chain.  Fix tail
			 * "chainHasLocks" flag if needed.
			 */
			if (curTuphdr.chainHasLocks)
			{
				clean_chain_has_locks_flag(lastLockOnlyUndoLocation,
										   pageTuphdr,
										   blkno);
				lastLockOnlyUndoLocation = InvalidUndoLocation;
			}

			if (!result)
			{
				*conflictTuphdr = finalTuphdr;
				*conflictUndoLocation = finalUndoLocation;
			}
			return result;
		}

		if (!delete_record)
		{
			/*
			 * We should be able to find a CSN-record or invalid undo location
			 * before running out of undo records.
			 */
			Assert(UNDO_REC_EXISTS(undoLocation));

			/*
			 * Update previous location of lock-only record.
			 */
			if (XACT_INFO_IS_LOCK_ONLY(xactInfo))
				lastLockOnlyUndoLocation = undoLocation;

			prevChainHasLocks = curTuphdr.chainHasLocks;
			get_prev_leaf_header_from_undo(&curTuphdr, false);
		}

		curUndoLocation = undoLocation;
		xactInfo = curTuphdr.xactInfo;
		xactMode = XACT_INFO_GET_LOCK_MODE(xactInfo);
		if (ROW_LOCKS_CONFLICT(xactMode, mode))
		{
			xactIsFinal = xactIsFinished = XACT_INFO_IS_FINISHED(xactInfo);
		}
		else
		{
			CommitSeqNo		csn = XACT_INFO_MAP_CSN(xactInfo);

			xactIsFinished = !COMMITSEQNO_IS_INPROGRESS(csn);
			xactIsFinal = (csn < my_csn);
		}
		undoLocation = curTuphdr.undoLocation;

		if (prevChainHasLocks &&
			!curTuphdr.chainHasLocks &&
			!XACT_INFO_IS_LOCK_ONLY(xactInfo))
		{
			/*
			 * We have reached the end of "in-progress" undo chain.  Fix tail
			 * "chainHasLocks" flag if needed.
			 */
			clean_chain_has_locks_flag(lastLockOnlyUndoLocation,
									   pageTuphdr,
									   blkno);
			lastLockOnlyUndoLocation = InvalidUndoLocation;
		}

		if (!foundFinal && xactIsFinal)
		{
			finalTuphdr = curTuphdr;
			finalUndoLocation = curUndoLocation;
			foundFinal = true;
		}
	}

	if (!result)
	{
		*conflictTuphdr = finalTuphdr;
		*conflictUndoLocation = finalUndoLocation;
	}
	return result;
}

/*
 * Remove redudant row-level locks.
 */
void
remove_redundant_row_locks(BTreeLeafTuphdr *pageTuphdr,
						   BTreeLeafTuphdr *conflictTuphdrPtr,
						   UndoLocation *conflictTupHdrUndoLocation,
						   RowLockMode mode,
						   OXid my_oxid, OInMemoryBlkno blkno,
						   UndoLocation savepointUndoLocation)
{
	BTreeLeafTuphdr tuphdr = *pageTuphdr;
	OTupleXactInfo xactInfo = tuphdr.xactInfo;
	bool		chainHasLocks = tuphdr.chainHasLocks,
				xactIsFinished = XACT_INFO_IS_FINISHED(xactInfo);
	UndoLocation undoLocation = tuphdr.undoLocation,
				prevUndoLoc = InvalidUndoLocation,
				lastLockOnlyUndoLocation = InvalidUndoLocation;
	UndoLocation retainedUndoLocation = get_snapshot_retained_undo_location();

	while ((!xactIsFinished || chainHasLocks) &&
		   undoLocation >= retainedUndoLocation &&
		   UndoLocationIsValid(undoLocation))
	{
		/*
		 * We should be able to find at CSN-record or invalid undo location
		 * before running out of undo records.
		 */
		Assert(UNDO_REC_EXISTS(undoLocation));

		get_prev_leaf_header_from_undo(&tuphdr, false);

		if (XACT_INFO_IS_LOCK_ONLY(xactInfo) && XACT_INFO_GET_OXID(xactInfo) == my_oxid)
		{
			bool		delete_record = false;

			Assert(UndoLocationIsValid(undoLocation) && UNDO_REC_EXISTS(undoLocation));

			if (XACT_INFO_GET_LOCK_MODE(xactInfo) <= mode &&
				(!UndoLocationIsValid(savepointUndoLocation) ||
				 (UndoLocationIsValid(undoLocation) &&
				  undoLocation >= savepointUndoLocation)))
				delete_record = true;

			if (delete_record)
			{
				if (*conflictTupHdrUndoLocation == undoLocation)
				{
					*conflictTuphdrPtr = tuphdr;
					*conflictTupHdrUndoLocation = prevUndoLoc;
				}
				if (!UndoLocationIsValid(prevUndoLoc))
				{
					page_block_reads(blkno);
					pageTuphdr->xactInfo = tuphdr.xactInfo;
					pageTuphdr->undoLocation = tuphdr.undoLocation;
				}
				else
				{
					/*
					 * Update chainHasLocks flag of the next undo records if
					 * needed.
					 */
					if (XACT_INFO_IS_LOCK_ONLY(xactInfo) && !chainHasLocks)
					{
						clean_chain_has_locks_flag(lastLockOnlyUndoLocation,
												   pageTuphdr,
												   blkno);
					}
					update_leaf_header_in_undo(&tuphdr, prevUndoLoc);
				}
			}
		}

		/*
		 * Update last location of lock-only record.
		 */
		if (XACT_INFO_IS_LOCK_ONLY(xactInfo))
			lastLockOnlyUndoLocation = prevUndoLoc;

		prevUndoLoc = undoLocation;
		xactInfo = tuphdr.xactInfo;
		xactIsFinished = XACT_INFO_IS_FINISHED(xactInfo);
		undoLocation = tuphdr.undoLocation;
		chainHasLocks = tuphdr.chainHasLocks;
	}
}

/*
 * Finds first non-lock-only undo record and returns pointer to it.  Returns
 * NULL if such record is not found.
 */
UndoLocation
find_non_lock_only_undo_record(BTreeLeafTuphdr *tuphdr)
{
	OTupleXactInfo xactInfo = tuphdr->xactInfo;
	UndoLocation undoLocation = InvalidUndoLocation;

	while (XACT_INFO_IS_LOCK_ONLY(xactInfo) || !XACT_INFO_IS_FINISHED(xactInfo))
	{
		if (!XACT_INFO_IS_LOCK_ONLY(xactInfo))
			return undoLocation;

		/*
		 * We should be able to find non lock-only undo location before
		 * running out of undo records.
		 */
		undoLocation = tuphdr->undoLocation;
		if (!UndoLocationIsValid(undoLocation) || !UNDO_REC_EXISTS(undoLocation))
			return InvalidUndoLocation;
		get_prev_leaf_header_from_undo(tuphdr, false);
		xactInfo = tuphdr->xactInfo;
	}

	return undoLocation;
}

void
get_prev_leaf_header_from_undo(BTreeLeafTuphdr *tuphdr, bool inPage)
{
	BTreeLeafTuphdr prevTuphdr;

	Assert(UndoLocationIsValid(tuphdr->undoLocation));
	Assert(UNDO_REC_EXISTS(tuphdr->undoLocation));

	undo_read(tuphdr->undoLocation, sizeof(prevTuphdr), (Pointer) &prevTuphdr);

	if (!XACT_INFO_IS_LOCK_ONLY(tuphdr->xactInfo) || !inPage)
	{
		*tuphdr = prevTuphdr;
	}
	else
	{
		tuphdr->xactInfo = prevTuphdr.xactInfo;
		tuphdr->undoLocation = prevTuphdr.undoLocation;
		tuphdr->chainHasLocks = prevTuphdr.chainHasLocks;
	}
}

void
get_prev_leaf_header_and_tuple_from_undo(BTreeLeafTuphdr *tuphdr,
										 OTuple *tuple,
										 LocationIndex sizeAvailable)
{
	BTreeModifyUndoStackItem item;
	LocationIndex tupleSize;
	UndoLocation undoLocation = tuphdr->undoLocation;

	Assert(UndoLocationIsValid(undoLocation) && UNDO_REC_EXISTS(undoLocation));

	undo_read(tuphdr->undoLocation - offsetof(BTreeModifyUndoStackItem, tuphdr),
			  sizeof(BTreeModifyUndoStackItem),
			  (Pointer) &item);
	Assert(item.header.type == ModifyUndoItemType);
	Assert(item.action == BTreeOperationUpdate);

	*tuphdr = item.tuphdr;
	tuple->formatFlags = tuphdr->formatFlags;
	tupleSize = item.header.itemSize - sizeof(BTreeModifyUndoStackItem);
	if (sizeAvailable == 0)
		tuple->data = palloc(tupleSize);
	Assert(sizeAvailable == 0 || sizeAvailable >= tupleSize);
	undo_read(undoLocation + BTreeLeafTuphdrSize,
			  tupleSize,
			  tuple->data);
	tuphdr->formatFlags = 0;
}

void
update_leaf_header_in_undo(BTreeLeafTuphdr *tuphdr, UndoLocation location)
{
	Assert(UndoLocationIsValid(location) && UNDO_REC_EXISTS(location));

	undo_write(location, sizeof(*tuphdr), (Pointer) tuphdr);
}
