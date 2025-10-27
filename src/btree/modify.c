/*-------------------------------------------------------------------------
 *
 * modify.c
 *		Routines for OrioleDB B-tree modification.
 *
 * Copyright (c) 2021-2025, Oriole DB Inc.
 * Copyright (c) 2025, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/btree/modify.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "orioledb.h"

#include "btree/find.h"
#include "btree/insert.h"
#include "btree/io.h"
#include "btree/merge.h"
#include "btree/modify.h"
#include "btree/page_chunks.h"
#include "btree/undo.h"
#include "catalog/o_tables.h"
#include "recovery/recovery.h"
#include "recovery/wal.h"
#include "transam/undo.h"
#include "transam/oxid.h"
#include "utils/page_pool.h"
#include "utils/stopevent.h"

#include "miscadmin.h"

#define IsRelationTree(desc) (ORelOidsIsValid(desc->oids) && !IS_SYS_TREE_OIDS(desc->oids))

/*
 * Context for o_btree_modify_internal()
 */
typedef struct
{
	OBTreeFindPageContext *pageFindContext;
	OTuple		tuple;
	BTreeKeyType tupleType;
	BTreeLeafTuphdr leafTuphdr;
	BTreeLeafTuphdr conflictTupHdr;
	bool		replace;
	UndoLocation conflictUndoLocation;
	OXid		opOxid;
	CommitSeqNo opCsn;
	RowLockMode lockMode;
	LOCKTAG		hwLockTag;
	LOCKMODE	hwLockMode;
	bool		needsUndo;
	int			pageReserveKind;
	int			cmp;
	BTreeModifyLockStatus lockStatus;
	bool		pagesAreReserved;
	bool		undoIsReserved;
	BTreeOperationType action;
	Pointer		key;
	BTreeKeyType keyType;
	UndoLocation savepointUndoLocation;
	BTreeModifyCallbackInfo *callbackInfo;
} BTreeModifyInternalContext;

typedef enum ConflictResolution
{
	ConflictResolutionOK,
	ConflictResolutionRetry,
	ConflictResolutionFound
} ConflictResolution;

BTreeModifyCallbackInfo nullCallbackInfo =
{
	.waitCallback = NULL,
	.modifyCallback = NULL,
	.modifyDeletedCallback = NULL,
	.needsUndoForSelfCreated = false,
	.arg = NULL
};

static const LOCKMODE hwLockModes[] = {AccessShareLock, RowShareLock, ExclusiveLock, AccessExclusiveLock};

static void unlock_release(BTreeModifyInternalContext *context, bool unlock);
static ConflictResolution o_btree_modify_handle_conflicts(BTreeModifyInternalContext *context);
static OBTreeModifyResult o_btree_modify_handle_tuple_not_found(BTreeModifyInternalContext *context);
static bool o_btree_modify_item_rollback(BTreeModifyInternalContext *context);
static void o_btree_modify_insert_update(BTreeModifyInternalContext *context);
static void o_btree_modify_add_undo_record(BTreeModifyInternalContext *context);
static OBTreeModifyResult o_btree_modify_delete(BTreeModifyInternalContext *context);
static OBTreeModifyResult o_btree_modify_lock(BTreeModifyInternalContext *context);
static Jsonb *prepare_modify_start_params(BTreeDescr *desc);
static OBTreeModifyResult o_btree_normal_modify(BTreeDescr *desc,
												BTreeOperationType action,
												OTuple tuple, BTreeKeyType tupleType,
												Pointer key, BTreeKeyType keyType,
												OXid opOxid,
												CommitSeqNo opCsn,
												RowLockMode lockMode,
												BTreeLocationHint *hint,
												BTreeLeafTupleDeletedStatus deleted,
												BTreeModifyCallbackInfo *callbackInfo);

/*
 * Perform modification of btree leaf tuple, when page is alredy located
 * and locked, all reservations are done.
 */
static OBTreeModifyResult
o_btree_modify_internal(OBTreeFindPageContext *pageFindContext,
						BTreeOperationType action,
						OTuple _tuple, BTreeKeyType tupleType,
						Pointer key, BTreeKeyType keyType,
						OXid opOxid, CommitSeqNo opCsn,
						RowLockMode _lockMode,
						BTreeLeafTupleDeletedStatus deleted,
						int pageReserveKind,
						BTreeModifyCallbackInfo *callbackInfo)
{
	BTreeDescr *desc = pageFindContext->desc;
	Page		page;
	BTreePageItemLocator loc;
	OInMemoryBlkno blkno;
	OBTreeModifyResult result = OBTreeModifyResultInserted;
	OTuple		curTuple;
	BTreeLeafTuphdr *tuphdr;
	BTreeModifyInternalContext context;
	OXid		tupleOxid = OXidIsValid(opOxid) ? opOxid : BootstrapTransactionId;

	context.tuple = _tuple;
	context.tupleType = tupleType;
	context.pageFindContext = pageFindContext;
	context.replace = false;
	context.opOxid = opOxid;
	context.opCsn = opCsn;
	context.lockMode = _lockMode;
	context.hwLockMode = NoLock;
	context.lockStatus = BTreeModifyNoLock;
	context.action = action;
	context.key = key;
	context.keyType = keyType;
	context.savepointUndoLocation = get_subxact_undo_location(desc->undoType);
	context.pageReserveKind = pageReserveKind;
	context.callbackInfo = callbackInfo;

	Assert(callbackInfo);
	Assert((action != BTreeOperationInsert) || (tupleType == BTreeKeyLeafTuple));
	Assert((action == BTreeOperationLock) || (context.lockMode >= RowLockNoKeyUpdate));
	Assert((deleted == BTreeLeafTupleNonDeleted) || (action == BTreeOperationDelete));

	context.pagesAreReserved = (action != BTreeOperationDelete);
	context.undoIsReserved = (desc->undoType != UndoLogNone);

	/* Undo should be reserved for transactional operations */
	Assert(OXidIsValid(opOxid) == context.undoIsReserved);

retry:

	context.needsUndo = desc->undoType != UndoLogNone;
	if (!(callbackInfo && callbackInfo->needsUndoForSelfCreated) &&
		OXidIsValid(desc->createOxid) &&
		desc->createOxid == opOxid &&
		!UndoLocationIsValid(context.savepointUndoLocation))
		context.needsUndo = false;
	context.leafTuphdr.deleted = deleted;
	context.leafTuphdr.undoLocation = InvalidUndoLocation;
	context.leafTuphdr.formatFlags = 0;
	context.leafTuphdr.chainHasLocks = false;
	context.leafTuphdr.xactInfo = OXID_GET_XACT_INFO(tupleOxid, context.lockMode, false);

	blkno = pageFindContext->items[pageFindContext->index].blkno;
	loc = pageFindContext->items[pageFindContext->index].locator;
	page = O_GET_IN_MEMORY_PAGE(blkno);
	Assert(page_is_locked(blkno));

	if (!BTREE_PAGE_LOCATOR_IS_VALID(page, &loc))
		return o_btree_modify_handle_tuple_not_found(&context);

	BTREE_PAGE_READ_LEAF_ITEM(tuphdr, curTuple, page, &loc);
	Assert(tuphdr != NULL);
	context.cmp = o_btree_cmp(desc, key, keyType, &curTuple, BTreeKeyLeafTuple);

	/* Trees without undo cannot have row locks */
	if (desc->undoType == UndoLogNone)
	{
		context.conflictTupHdr = *tuphdr;
		context.conflictUndoLocation = InvalidUndoLocation;
	}
	else if (context.cmp == 0)
	{
		ConflictResolution resolution;

		resolution = o_btree_modify_handle_conflicts(&context);

		if (resolution == ConflictResolutionFound)
			return OBTreeModifyResultFound;
		else if (resolution == ConflictResolutionRetry)
			goto retry;
	}

	Assert(page_is_locked(blkno));

	if (context.cmp != 0)
		return o_btree_modify_handle_tuple_not_found(&context);

	if (tuphdr->deleted == BTreeLeafTupleNonDeleted)
	{
		/* Existing (non-deleted) tuple is found */
		OBTreeModifyCallbackAction cbAction = OBTreeCallbackActionDoNothing;
		RowLockMode prev_lock_mode = context.lockMode;

		/*
		 * We should have set conflictTupHdr in the (cmp == 0) branch above.
		 */
		if (callbackInfo->modifyCallback)
		{
			BTreeLocationHint cbHint;

			cbHint.blkno = pageFindContext->items[pageFindContext->index].blkno;
			cbHint.pageChangeCount = pageFindContext->items[pageFindContext->index].pageChangeCount;
			cbAction = callbackInfo->modifyCallback(desc, curTuple,
													&context.tuple, opOxid, context.conflictTupHdr.xactInfo,
													context.conflictTupHdr.undoLocation,
													&context.lockMode, &cbHint, callbackInfo->arg);
			context.leafTuphdr.xactInfo = OXID_GET_XACT_INFO(tupleOxid, context.lockMode, false);
		}

		if (cbAction == OBTreeCallbackActionUndo)
		{
			(void) o_btree_modify_item_rollback(&context);
			goto retry;
		}

		Assert(page_is_locked(blkno));

		if (callbackInfo->modifyCallback || (action == BTreeOperationInsert ||
											 action == BTreeOperationUpdate ||
											 action == BTreeOperationLock))
		{
			if (cbAction == OBTreeCallbackActionDoNothing)
			{
				unlock_release(&context, true);
				return OBTreeModifyResultFound;
			}
			else
			{
				if (context.lockMode > prev_lock_mode)
				{
					OFindPageResult result PG_USED_FOR_ASSERTS_ONLY;

					unlock_page(blkno);

					result = refind_page(pageFindContext,
										 key,
										 keyType,
										 0,
										 pageFindContext->items[pageFindContext->index].blkno,
										 pageFindContext->items[pageFindContext->index].pageChangeCount);
					Assert(result == OFindPageResultSuccess);
					goto retry;
				}

				if (cbAction == OBTreeCallbackActionUpdate)
				{
					Assert(tupleType == BTreeKeyLeafTuple);
					context.replace = true;
					result = OBTreeModifyResultUpdated;
				}
				else if (cbAction == OBTreeCallbackActionLock)
				{
					action = BTreeOperationLock;
				}
				else
				{
					Assert(cbAction == OBTreeCallbackActionDelete);
					action = BTreeOperationDelete;
				}
			}
		}

		Assert((action == BTreeOperationLock) || (context.lockMode >= RowLockNoKeyUpdate));

		if (action == BTreeOperationDelete)
			return o_btree_modify_delete(&context);
		else if (action == BTreeOperationLock)
			return o_btree_modify_lock(&context);
	}
	else if (tuphdr->deleted != BTreeLeafTupleNonDeleted)
	{
		/*
		 * We should have set conflictTupHdr in the (cmp == 0) branch above.
		 */

		if (action == BTreeOperationInsert && callbackInfo->modifyDeletedCallback)
		{
			OBTreeModifyCallbackAction cbAction = OBTreeCallbackActionDoNothing;
			BTreeLocationHint cbHint;

			cbHint.blkno = pageFindContext->items[pageFindContext->index].blkno;
			cbHint.pageChangeCount = pageFindContext->items[pageFindContext->index].pageChangeCount;
			cbAction = callbackInfo->modifyDeletedCallback(desc, curTuple,
														   &context.tuple, opOxid,
														   context.conflictTupHdr.xactInfo,
														   context.conflictTupHdr.deleted,
														   context.conflictTupHdr.undoLocation,
														   &context.lockMode, &cbHint, callbackInfo->arg);
			context.leafTuphdr.xactInfo = OXID_GET_XACT_INFO(tupleOxid, context.lockMode, false);

			if (cbAction == OBTreeCallbackActionUndo)
			{
				(void) o_btree_modify_item_rollback(&context);
				goto retry;
			}

			if (cbAction == OBTreeCallbackActionDoNothing)
			{
				unlock_release(&context, true);
				return OBTreeModifyResultNotFound;
			}
			Assert(cbAction == OBTreeCallbackActionUpdate);
		}

		/*
		 * Deleted tuple found, we only can handle insert at this point. This
		 * insert essentially becomes update.
		 */
		if (action == BTreeOperationInsert)
		{
			/*
			 * There is no anything to undo for UndoLogNone trees so just
			 * proceed with replacing while page still locked
			 */
			if (!context.needsUndo && desc->undoType != UndoLogNone)
			{
				/*
				 * If we don't need undo, just revert the deletion and then
				 * continue with normal insert (with undo).
				 */
				(void) o_btree_modify_item_rollback(&context);
				context.needsUndo = true;
			}
			else if (IsolationUsesXactSnapshot() && IsRelationTree(desc))
			{
				if (XACT_INFO_MAP_CSN(context.conflictTupHdr.xactInfo) >= opCsn)
				{
					ereport(ERROR,
							(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
							 errmsg("could not serialize access due to concurrent update")));
				}
			}
			context.replace = true;
		}
		else
		{
			unlock_release(&context, true);
			if (callbackInfo->modifyDeletedCallback)
				callbackInfo->modifyDeletedCallback(desc, curTuple,
													&context.tuple, opOxid,
													context.conflictTupHdr.xactInfo,
													context.conflictTupHdr.deleted,
													context.conflictTupHdr.undoLocation,
													&context.lockMode, NULL,
													callbackInfo->arg);
			return OBTreeModifyResultNotFound;
		}
	}

	Assert(tupleType == BTreeKeyLeafTuple);

	o_btree_modify_insert_update(&context);
	unlock_release(&context, false);
	return result;
}

static void
unlock_release(BTreeModifyInternalContext *context, bool unlock)
{
	OBTreeFindPageContext *pageFindContext = context->pageFindContext;
	BTreeDescr *desc = pageFindContext->desc;
	OInMemoryBlkno blkno;

	blkno = pageFindContext->items[pageFindContext->index].blkno;

	if (unlock)
		unlock_page(blkno);
	if (context->undoIsReserved)
	{
		release_undo_size(desc->undoType);
		if (GET_PAGE_LEVEL_UNDO_TYPE(desc->undoType) != desc->undoType)
			release_undo_size(GET_PAGE_LEVEL_UNDO_TYPE(desc->undoType));
	}
	if (context->pagesAreReserved)
		ppool_release_reserved(desc->ppool,
							   PPOOL_KIND_GET_MASK(context->pageReserveKind));
	if (context->hwLockMode != NoLock)
		LockRelease(&context->hwLockTag, context->hwLockMode, false);
}

static void
wait_for_tuple(BTreeDescr *desc, OTuple tuple, OXid oxid,
			   RowLockMode lockMode, BTreeModifyLockStatus lockStatus,
			   LOCKTAG *hwLockTag, LOCKMODE *hwLockMode)
{
	uint32		hash;

	/*
	 * Acquire the lock, if necessary (but skip it when we're requesting a
	 * lock and already have one; avoids deadlock).
	 */
	if (*hwLockMode == NoLock && lockStatus == BTreeModifyNoLock)
	{
		hash = o_btree_hash(desc, tuple, BTreeKeyLeafTuple);

		SET_LOCKTAG_TUPLE(*hwLockTag,
						  desc->oids.datoid,
						  desc->oids.reloid,
						  hash,
						  0);
		*hwLockMode = hwLockModes[lockMode];

		(void) LockAcquire(hwLockTag, *hwLockMode, false, false);
	}

	wait_for_oxid(oxid, false);
}

static ConflictResolution
o_btree_modify_handle_conflicts(BTreeModifyInternalContext *context)
{
	bool		haveRedundantRowLocks = false;
	OBTreeFindPageContext *pageFindContext = context->pageFindContext;
	BTreeDescr *desc = pageFindContext->desc;
	OInMemoryBlkno blkno;
	BTreePageItemLocator *loc;
	Page		page;
	OTuple		curTuple;
	BTreeLeafTuphdr *tuphdr;

	blkno = pageFindContext->items[pageFindContext->index].blkno;
	loc = &pageFindContext->items[pageFindContext->index].locator;
	page = O_GET_IN_MEMORY_PAGE(blkno);

	BTREE_PAGE_READ_LEAF_ITEM(tuphdr, curTuple, page, loc);

	if (row_lock_conflicts(tuphdr,
						   &context->conflictTupHdr,
						   desc->undoType,
						   &context->conflictUndoLocation,
						   context->lockMode, context->opOxid, context->opCsn,
						   blkno, context->savepointUndoLocation,
						   &haveRedundantRowLocks, &context->lockStatus))
	{
		OTupleXactInfo xactInfo = context->conflictTupHdr.xactInfo;
		OXid		oxid = XACT_INFO_GET_OXID(xactInfo);

		if (oxid == context->opOxid)
		{
			if (context->action == BTreeOperationLock ||
				(UndoLocationIsValid(context->savepointUndoLocation) &&
				 (!UndoLocationIsValid(context->conflictTupHdr.undoLocation) ||
				  context->conflictTupHdr.undoLocation < context->savepointUndoLocation)) ||
				o_btree_needs_undo(desc, context->action, curTuple, xactInfo,
								   tuphdr->deleted != BTreeLeafTupleNonDeleted,
								   context->tuple, context->opOxid))
			{
				context->needsUndo = true;
			}
			else
			{
				if (XACT_INFO_GET_LOCK_MODE(xactInfo) > context->lockMode)
				{
					/*
					 * Upgrade our lock mode if we're going to replace our own
					 * undo item.
					 */
					Assert(OXidIsValid(context->opOxid));
					context->lockMode = XACT_INFO_GET_LOCK_MODE(xactInfo);
					context->leafTuphdr.xactInfo = OXID_GET_XACT_INFO(context->opOxid,
																	  context->lockMode,
																	  false);
				}
				context->needsUndo = false;
			}
		}
		else
		{
			CommitSeqNo csn = oxid_get_csn(oxid, false);

			if (XACT_INFO_IS_LOCK_ONLY(xactInfo) && (COMMITSEQNO_IS_ABORTED(csn) ||
													 COMMITSEQNO_IS_NORMAL(csn) ||
													 COMMITSEQNO_IS_FROZEN(csn)))
			{
				/*
				 * Normally row_lock_conflicts() should have lock-only records
				 * of committed and aborted transactions already removed from
				 * the undo chain.  But if locker transaction commit or abort
				 * concurrently, then retry.
				 */
				return ConflictResolutionRetry;
			}

			if (COMMITSEQNO_IS_ABORTED(csn))
			{
				/*
				 * Transaction changes should be undone by the transaction
				 * owner.  But we rollback those changes ourself instead of
				 * waiting.
				 */
				START_CRIT_SECTION();
				page_block_reads(blkno);
				if (!page_item_rollback(desc, page, loc, true,
										&context->conflictTupHdr,
										context->conflictUndoLocation))
					context->cmp = -1;
				MARK_DIRTY(desc, blkno);
				END_CRIT_SECTION();
			}
			else if (COMMITSEQNO_IS_NORMAL(csn) || COMMITSEQNO_IS_FROZEN(csn))
			{
				/*
				 * Check for serialization conflicts.
				 *
				 * TODO: check for such conflicts in page-level undo as well.
				 */
				if (csn >= context->opCsn && IsolationUsesXactSnapshot() &&
					IsRelationTree(desc))
				{
					ereport(ERROR,
							(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
							 errmsg("could not serialize access due to concurrent update")));
				}
			}
			else
			{
				/*
				 * Conflicting transaction is in-progress.  If the callback is
				 * provided, ask it what to do.  Just wait otherwise.
				 */
				OBTreeWaitCallbackAction cbAction = OBTreeCallbackActionXidWait;
				OFindPageResult result PG_USED_FOR_ASSERTS_ONLY;

				Assert(COMMITSEQNO_IS_INPROGRESS(csn));

				if (context->callbackInfo->waitCallback)
				{
					BTreeLocationHint cbHint;

					cbHint.blkno = pageFindContext->items[pageFindContext->index].blkno;
					cbHint.pageChangeCount = pageFindContext->items[pageFindContext->index].pageChangeCount;
					cbAction = context->callbackInfo->waitCallback(desc,
																   curTuple, &context->tuple, oxid,
																   context->conflictTupHdr.xactInfo,
																   context->conflictTupHdr.undoLocation,
																   &context->lockMode, &cbHint,
																   context->callbackInfo->arg);
				}

				unlock_page(blkno);

				Assert(cbAction <= OBTreeCallbackActionXidExit);

				if (cbAction == OBTreeCallbackActionXidWait)
					wait_for_tuple(desc, curTuple, oxid,
								   context->lockMode,
								   context->lockStatus,
								   &context->hwLockTag,
								   &context->hwLockMode);
				else if (cbAction == OBTreeCallbackActionXidExit)
					return ConflictResolutionFound;
				else
				{
					Assert(cbAction == OBTreeCallbackActionXidNoWait);
				}

				result = refind_page(pageFindContext,
									 context->key,
									 context->keyType,
									 0,
									 pageFindContext->items[pageFindContext->index].blkno,
									 pageFindContext->items[pageFindContext->index].pageChangeCount);
				Assert(result == OFindPageResultSuccess);
				return ConflictResolutionRetry;
			}

			/* Update tuple and header pointer after page_item_rollback() */
			BTREE_PAGE_READ_LEAF_ITEM(tuphdr, curTuple, page, loc);
		}
	}
	else if (IsolationUsesXactSnapshot() && IsRelationTree(desc))
	{
		/*
		 * Check for serialization conflicts.
		 *
		 * TODO: check for such conflicts in page-level undo as well.
		 */
		CommitSeqNo csn = XACT_INFO_MAP_CSN(context->conflictTupHdr.xactInfo);

		if (csn >= context->opCsn)
		{
			if (tuphdr->deleted == BTreeLeafTupleDeleted ||
				tuphdr->deleted == BTreeLeafTupleMovedPartitions)
				ereport(ERROR,
						(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
						 errmsg("could not serialize access due to concurrent delete")));
			else
				ereport(ERROR,
						(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
						 errmsg("could not serialize access due to concurrent update")));
		}
	}

	/*
	 * Remove redundant row-level locks if any.
	 */
	if (haveRedundantRowLocks &&
		!(context->action == BTreeOperationLock &&
		  context->lockStatus == BTreeModifySameOrStrongerLock))
	{
		remove_redundant_row_locks(tuphdr, &context->conflictTupHdr,
								   desc->undoType,
								   &context->conflictUndoLocation,
								   context->lockMode,
								   context->opOxid, blkno,
								   context->savepointUndoLocation);
	}

	if (!context->needsUndo)
		context->leafTuphdr.undoLocation = tuphdr->undoLocation;
	return ConflictResolutionOK;
}

static OBTreeModifyResult
o_btree_modify_handle_tuple_not_found(BTreeModifyInternalContext *context)
{
	/*
	 * Matching tuple is not found.
	 *
	 * Ideally, for IsolationUsesXactSnapshot() we should also check
	 * page-level undo for conflicting tuples.  But it's not implemented so
	 * far.
	 */
	if (context->action == BTreeOperationUpdate ||
		context->action == BTreeOperationDelete ||
		context->action == BTreeOperationLock)
	{
		unlock_release(context, true);
		return OBTreeModifyResultNotFound;
	}
	else
	{
		Assert(context->tupleType == BTreeKeyLeafTuple);

		o_btree_modify_insert_update(context);
		unlock_release(context, false);
		return OBTreeModifyResultInserted;
	}
}

static bool
o_btree_modify_item_rollback(BTreeModifyInternalContext *context)
{
	OBTreeFindPageContext *pageFindContext = context->pageFindContext;
	BTreeDescr *desc = pageFindContext->desc;
	OInMemoryBlkno blkno;
	BTreePageItemLocator loc;
	Page		page;
	bool		applyResult;

	blkno = pageFindContext->items[pageFindContext->index].blkno;
	loc = pageFindContext->items[pageFindContext->index].locator;
	page = O_GET_IN_MEMORY_PAGE(blkno);

	START_CRIT_SECTION();
	page_block_reads(blkno);
	applyResult = page_item_rollback(desc, page, &loc, false,
									 &context->conflictTupHdr,
									 context->conflictUndoLocation);
	MARK_DIRTY(desc, blkno);
	END_CRIT_SECTION();

	if (!applyResult)
	{
		btree_page_search(desc, page, context->key,
						  context->keyType, NULL, &loc);
		pageFindContext->items[pageFindContext->index].locator = loc;
	}

	return applyResult;
}


static void
o_btree_modify_insert_update(BTreeModifyInternalContext *context)
{
	OBTreeFindPageContext *pageFindContext = context->pageFindContext;
	BTreeDescr *desc = pageFindContext->desc;
	int			tuplen;

	if (context->undoIsReserved && context->needsUndo)
	{
		o_btree_modify_add_undo_record(context);
	}
	else if (!context->needsUndo)
	{
		BTreeLeafTuphdr *leafTuphdr = &context->leafTuphdr;

		if (desc->undoType == UndoLogRegular)
		{
			leafTuphdr->undoLocation = InvalidUndoLocation;
			leafTuphdr->undoLocation |= current_command_get_undo_location();
		}
	}

	if (desc->undoType == UndoLogRegular && !is_recovery_process())
	{
		Assert(undo_location_get_command(UndoLocationGetValue(context->leafTuphdr.undoLocation)) == o_get_current_command());
	}

	tuplen = o_btree_len(desc, context->tuple, OTupleLength);
	Assert(tuplen <= O_BTREE_MAX_TUPLE_SIZE);

	/* no more sense in that */
	BTREE_PAGE_FIND_UNSET(pageFindContext, FIX_LEAF_SPLIT);
	o_btree_insert_tuple_to_leaf(pageFindContext,
								 context->tuple, tuplen,
								 &context->leafTuphdr,
								 context->replace,
								 context->pageReserveKind);
}

static void
o_btree_modify_add_undo_record(BTreeModifyInternalContext *context)
{
	OBTreeFindPageContext *pageFindContext = context->pageFindContext;
	BTreeDescr *desc = pageFindContext->desc;
	BTreeLeafTuphdr *leafTuphdr = &context->leafTuphdr;
	UndoLocation undoLocation = InvalidUndoLocation;
	OInMemoryBlkno blkno;
	BTreePageItemLocator loc;
	Page		page;

	blkno = pageFindContext->items[pageFindContext->index].blkno;
	loc = pageFindContext->items[pageFindContext->index].locator;
	page = O_GET_IN_MEMORY_PAGE(blkno);

	if (context->replace)
	{
		/* Make undo item and connect it with page tuple */
		OTuple		curTuple;
		BTreeLeafTuphdr *tuphdr;

		BTREE_PAGE_READ_LEAF_ITEM(tuphdr, curTuple, page, &loc);

		undoLocation = make_undo_record(desc, curTuple, true,
										BTreeOperationUpdate, blkno,
										O_PAGE_GET_CHANGE_COUNT(page),
										tuphdr);
		leafTuphdr->undoLocation = undoLocation;
		leafTuphdr->chainHasLocks = tuphdr->chainHasLocks ||
			XACT_INFO_IS_LOCK_ONLY(tuphdr->xactInfo);
	}
	else
	{
		/* Still need the undo item to deal with transaction rollback */
		undoLocation = make_undo_record(desc, context->tuple, true,
										BTreeOperationInsert, blkno,
										O_PAGE_GET_CHANGE_COUNT(page),
										NULL);
		if (desc->undoType == UndoLogRegular)
		{
			leafTuphdr->undoLocation = InvalidUndoLocation;
			leafTuphdr->undoLocation |= current_command_get_undo_location();
		}
	}
}

static OBTreeModifyResult
o_btree_modify_delete(BTreeModifyInternalContext *context)
{
	OBTreeFindPageContext *pageFindContext = context->pageFindContext;
	BTreeDescr *desc = pageFindContext->desc;
	uint32		pageChangeCount;
	UndoLocation undoLocation;
	OInMemoryBlkno blkno;
	BTreePageItemLocator loc;
	Page		page;
	OTuple		curTuple;
	BTreeLeafTuphdr *tuphdr;

	blkno = pageFindContext->items[pageFindContext->index].blkno;
	loc = pageFindContext->items[pageFindContext->index].locator;
	page = O_GET_IN_MEMORY_PAGE(blkno);

	BTREE_PAGE_READ_LEAF_ITEM(tuphdr, curTuple, page, &loc);

	if (!context->needsUndo)
	{
		bool		stillExists;

		stillExists = o_btree_modify_item_rollback(context);

		if (stillExists)
		{
			BTREE_PAGE_READ_LEAF_ITEM(tuphdr, curTuple, page, &loc);
			Assert(tuphdr != NULL);
			stillExists = (tuphdr->deleted == BTreeLeafTupleNonDeleted);
		}

		if (!stillExists)
		{
			/* Already deleted */
			unlock_release(context, true);

			return OBTreeModifyResultDeleted;
		}
		else
		{
			/*
			 * We rollback our own changes to the version existed before.
			 * Thus, we need an undo record to modify it.
			 */
			context->needsUndo = true;
		}
	}

	if (context->undoIsReserved && context->needsUndo)
	{
		OTuple		key;
		bool		key_is_tuple;

		if (context->tupleType == BTreeKeyNonLeafKey)
		{
			key = context->tuple;
			key_is_tuple = false;
		}
		else
		{
			key = curTuple;
			key_is_tuple = true;
		}

		pageChangeCount = O_PAGE_GET_CHANGE_COUNT(page);
		undoLocation = make_undo_record(desc, key, key_is_tuple,
										BTreeOperationDelete, blkno,
										pageChangeCount, tuphdr);
	}
	else
	{
		undoLocation = InvalidUndoLocation;
	}

	START_CRIT_SECTION();
	page_block_reads(blkno);

	tuphdr->chainHasLocks = tuphdr->chainHasLocks ||
		XACT_INFO_IS_LOCK_ONLY(tuphdr->xactInfo);
	tuphdr->undoLocation = undoLocation;
	tuphdr->xactInfo = context->leafTuphdr.xactInfo;
	if (context->leafTuphdr.deleted == BTreeLeafTupleNonDeleted)
		tuphdr->deleted = BTreeLeafTupleDeleted;
	else
		tuphdr->deleted = context->leafTuphdr.deleted;

	/* Bridge index deleted tuples not treated as vacated */
	if (desc->type != oIndexBridge)
		PAGE_ADD_N_VACATED(page,
						   BTreeLeafTuphdrSize +
						   MAXALIGN(o_btree_len(desc, curTuple, OTupleLength)));

	MARK_DIRTY(desc, blkno);

	END_CRIT_SECTION();

	if (!OXidIsValid(context->opOxid) && is_page_too_sparse(desc, page))
	{
		(void) btree_try_merge_and_unlock(desc, blkno, false, false);
		unlock_release(context, false);
	}
	else
	{
		unlock_release(context, true);
	}

	return OBTreeModifyResultDeleted;
}

static OBTreeModifyResult
o_btree_modify_lock(BTreeModifyInternalContext *context)
{
	OBTreeFindPageContext *pageFindContext = context->pageFindContext;
	BTreeDescr *desc = pageFindContext->desc;
	UndoLocation undoLocation;
	uint32		pageChangeCount;
	OTuple		key;
	bool		key_is_tuple;
	OInMemoryBlkno blkno;
	BTreePageItemLocator loc;
	Page		page;
	OTuple		curTuple;
	BTreeLeafTuphdr *tuphdr;

	blkno = pageFindContext->items[pageFindContext->index].blkno;
	loc = pageFindContext->items[pageFindContext->index].locator;
	page = O_GET_IN_MEMORY_PAGE(blkno);

	BTREE_PAGE_READ_LEAF_ITEM(tuphdr, curTuple, page, &loc);

	if (context->lockStatus == BTreeModifySameOrStrongerLock)
	{
		unlock_release(context, true);
		return OBTreeModifyResultLocked;
	}

	Assert(context->needsUndo);
	Assert(context->undoIsReserved);
	Assert(OXidIsValid(context->opOxid));

	if (context->tupleType == BTreeKeyNonLeafKey)
	{
		key = context->tuple;
		key_is_tuple = false;
	}
	else
	{
		key = curTuple;
		key_is_tuple = true;
	}

	pageChangeCount = O_PAGE_GET_CHANGE_COUNT(page);
	undoLocation = make_undo_record(desc, key, key_is_tuple,
									BTreeOperationLock, blkno,
									pageChangeCount, tuphdr);

	START_CRIT_SECTION();
	page_block_reads(blkno);

	tuphdr->chainHasLocks = tuphdr->chainHasLocks ||
		XACT_INFO_IS_LOCK_ONLY(tuphdr->xactInfo);
	tuphdr->undoLocation = undoLocation;
	tuphdr->xactInfo = OXID_GET_XACT_INFO(context->opOxid,
										  context->lockMode,
										  true);
	tuphdr->deleted = BTreeLeafTupleNonDeleted;

	MARK_DIRTY(desc, blkno);
	END_CRIT_SECTION();
	unlock_release(context, true);

	return OBTreeModifyResultLocked;
}

static Jsonb *
prepare_modify_start_params(BTreeDescr *desc)
{
	JsonbParseState *state = NULL;
	Jsonb	   *res;

	MemoryContext mctx = MemoryContextSwitchTo(stopevents_cxt);

	pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
	btree_desc_stopevent_params_internal(desc, &state);
	res = JsonbValueToJsonb(pushJsonbValue(&state, WJB_END_OBJECT, NULL));
	MemoryContextSwitchTo(mctx);

	return res;
}

static void
reserve_undo_for_modification(UndoLogType undoType)
{
	if (undoType == UndoLogNone)
		return;

	if (GET_PAGE_LEVEL_UNDO_TYPE(undoType) == undoType)
	{
		(void) reserve_undo_size(undoType, O_MODIFY_UNDO_RESERVE_SIZE);
	}
	else
	{
		(void) reserve_undo_size(undoType, 2 * O_UPDATE_MAX_UNDO_SIZE);
		(void) reserve_undo_size(GET_PAGE_LEVEL_UNDO_TYPE(undoType), 2 * O_MAX_SPLIT_UNDO_IMAGE_SIZE);
	}
}

static OBTreeModifyResult
o_btree_normal_modify(BTreeDescr *desc, BTreeOperationType action,
					  OTuple tuple, BTreeKeyType tupleType,
					  Pointer key, BTreeKeyType keyType,
					  OXid opOxid, CommitSeqNo opCsn,
					  RowLockMode lockMode, BTreeLocationHint *hint,
					  BTreeLeafTupleDeletedStatus deleted,
					  BTreeModifyCallbackInfo *callbackInfo)
{
	OBTreeFindPageContext pageFindContext;
	int			pageReserveKind;
	Jsonb	   *params = NULL;
	OFindPageResult findResult;

	if (STOPEVENTS_ENABLED())
		params = prepare_modify_start_params(desc);
	STOPEVENT(STOPEVENT_MODIFY_START, params);

	/* No no key is separately given, use the tuple itself */
	if (key == NULL)
	{
		key = (Pointer) &tuple;
		keyType = tupleType;
	}

	reserve_undo_for_modification(desc->undoType);

	if (OIDS_EQ_SYS_TREE(desc->oids, SYS_TREES_SHARED_ROOT_INFO))
		pageReserveKind = PPOOL_RESERVE_SHARED_INFO_INSERT;
	else
		pageReserveKind = PPOOL_RESERVE_INSERT;

	if (action != BTreeOperationDelete)
		ppool_reserve_pages(desc->ppool, pageReserveKind, 2);

	init_page_find_context(&pageFindContext, desc, COMMITSEQNO_INPROGRESS,
						   BTREE_PAGE_FIND_MODIFY | BTREE_PAGE_FIND_FIX_LEAF_SPLIT);

	if (action == BTreeOperationInsert && tupleType == BTreeKeyLeafTuple)
	{
		pageFindContext.insertTuple = tuple;
		if (OXidIsValid(opOxid))
			pageFindContext.insertXactInfo = OXID_GET_XACT_INFO(opOxid, lockMode, false);
		else
			pageFindContext.insertXactInfo = OXID_GET_XACT_INFO(BootstrapTransactionId, lockMode, false);
	}

	if (hint && OInMemoryBlknoIsValid(hint->blkno))
		findResult = refind_page(&pageFindContext, key, keyType, 0, hint->blkno, hint->pageChangeCount);
	else
		findResult = find_page(&pageFindContext, key, keyType, 0);

	if (findResult == OFindPageResultInserted)
	{
		Assert(action == BTreeOperationInsert);
		Assert(tupleType == BTreeKeyLeafTuple);

		if (desc->undoType != UndoLogNone)
		{
			release_undo_size(desc->undoType);
			if (GET_PAGE_LEVEL_UNDO_TYPE(desc->undoType) != desc->undoType)
				release_undo_size(GET_PAGE_LEVEL_UNDO_TYPE(desc->undoType));
		}
		ppool_release_reserved(desc->ppool, PPOOL_RESERVE_INSERT);
		Assert(!have_locked_pages());
		return OBTreeModifyResultInserted;
	}
	Assert(findResult == OFindPageResultSuccess);

	return o_btree_modify_internal(&pageFindContext, action, tuple, tupleType,
								   key, keyType, opOxid, opCsn,
								   lockMode, deleted, pageReserveKind,
								   callbackInfo);
}

static bool
page_unique_check(BTreeDescr *desc, Page p, BTreePageItemLocator *locator,
				  Pointer key, OXid opOxid, OTupleXactInfo *xactInfo)
{
	(void) page_locator_find_real_item(p, NULL, locator);

	while (BTREE_PAGE_LOCATOR_IS_VALID(p, locator))
	{
		int			cmp;
		OTuple		tuple;
		BTreeLeafTuphdr *pageTuphdr,
					tuphdr;

		BTREE_PAGE_READ_LEAF_ITEM(pageTuphdr, tuple, p, locator);
		cmp = o_btree_cmp(desc, &tuple, BTreeKeyLeafTuple,
						  key, BTreeKeyUniqueUpperBound);
		if (cmp > 0)
			return false;

		tuphdr = *pageTuphdr;
		(void) find_non_lock_only_undo_record(desc->undoType, &tuphdr);
		if (XACT_INFO_OXID_EQ(tuphdr.xactInfo, opOxid) || XACT_INFO_IS_FINISHED(tuphdr.xactInfo))
		{
			if (tuphdr.deleted != BTreeLeafTupleNonDeleted)
			{
				BTREE_PAGE_LOCATOR_NEXT(p, locator);
				continue;
			}
			*xactInfo = tuphdr.xactInfo;
			return true;
		}

		*xactInfo = tuphdr.xactInfo;
		return true;
	}
	return false;
}

static bool
slowpath_unique_check(BTreeDescr *desc, OBTreeFindPageContext *pageFindContext,
					  Pointer key, OXid opOxid, OTupleXactInfo *xactInfo)
{
	Page		p;
	OFixedKey	hikey_buf;

	btree_find_context_from_modify_to_read(pageFindContext,
										   key, BTreeKeyUniqueLowerBound, 0);

	p = pageFindContext->img;

	while (true)
	{
		int			cmp;
		OTuple		hikey;

		if (page_unique_check(desc, p, &pageFindContext->items[pageFindContext->index].locator,
							  key, opOxid, xactInfo))
			return true;

		if (O_PAGE_IS(p, RIGHTMOST))
			break;

		BTREE_PAGE_GET_HIKEY(hikey, p);

		cmp = o_btree_cmp(desc, &hikey, BTreeKeyNonLeafKey,
						  key, BTreeKeyUniqueUpperBound);
		if (cmp > 0)
			break;

		(void) find_right_page(pageFindContext, &hikey_buf);

		/*
		 * Due to concurrent merges, some tuples might be lower than the
		 * unique key.  So, we can't just start from the beginning, but have
		 * to find the right position on the page.
		 */
		btree_page_search(desc, p, key, BTreeKeyUniqueLowerBound,
						  NULL, &pageFindContext->items[pageFindContext->index].locator);
	}
	return false;
}

OBTreeModifyResult
o_btree_insert_unique(BTreeDescr *desc, OTuple tuple, BTreeKeyType tupleType,
					  Pointer key, BTreeKeyType keyType,
					  OXid opOxid, CommitSeqNo opCsn,
					  RowLockMode lockMode, BTreeLocationHint *hint,
					  BTreeModifyCallbackInfo *callbackInfo)
{
	OBTreeFindPageContext pageFindContext;
	int			pageReserveKind;
	bool		fastpath;
	Page		p;
	OInMemoryBlkno blkno;
	uint32		pageChangeCount;
	LWLock	   *uniqueLock;
	OBTreeModifyResult result = 456;
	Jsonb	   *params = NULL;
	OFindPageResult findResult PG_USED_FOR_ASSERTS_ONLY;

	if (STOPEVENTS_ENABLED())
		params = prepare_modify_start_params(desc);
	STOPEVENT(STOPEVENT_MODIFY_START, params);

	Assert(key != NULL && keyType == BTreeKeyBound);

	reserve_undo_for_modification(desc->undoType);

	if (OIDS_EQ_SYS_TREE(desc->oids, SYS_TREES_SHARED_ROOT_INFO))
		pageReserveKind = PPOOL_RESERVE_SHARED_INFO_INSERT;
	else
		pageReserveKind = PPOOL_RESERVE_INSERT;

	ppool_reserve_pages(desc->ppool, pageReserveKind, 2);

	init_page_find_context(&pageFindContext, desc, COMMITSEQNO_INPROGRESS,
						   BTREE_PAGE_FIND_MODIFY |
						   BTREE_PAGE_FIND_IMAGE |
						   BTREE_PAGE_FIND_FIX_LEAF_SPLIT);

	if (hint && OInMemoryBlknoIsValid(hint->blkno))
		findResult = refind_page(&pageFindContext, key,
								 BTreeKeyUniqueLowerBound, 0,
								 hint->blkno, hint->pageChangeCount);
	else
		findResult = find_page(&pageFindContext, key,
							   BTreeKeyUniqueLowerBound, 0);

	Assert(findResult == OFindPageResultSuccess);

retry:

	fastpath = false;
	blkno = pageFindContext.items[pageFindContext.index].blkno;
	pageChangeCount = pageFindContext.items[pageFindContext.index].pageChangeCount;
	p = O_GET_IN_MEMORY_PAGE(blkno);
	if (O_PAGE_IS(p, RIGHTMOST))
	{
		fastpath = true;
	}
	else
	{
		OTuple		hikey;

		BTREE_PAGE_GET_HIKEY(hikey, p);
		fastpath = (o_btree_cmp(desc, &hikey, BTreeKeyNonLeafKey,
								key, BTreeKeyUniqueUpperBound) >= 0);
	}

	uniqueLock = &unique_locks[o_btree_unique_hash(desc, tuple) % num_unique_locks].lock;

	/*---
	 * We can do fast path unique check if we know that the required key range
	 * resides the single page, and we managed to take a unique lwlock
	 * simultaneusly.
	 *
	 * It might seem that we don't need unique lwlock as soon as we see all the
	 * key range in the locked page.  However, consider the following example.
	 *
	 * s1: Unique lwlock acquire
	 * s1: Slow path check
	 * Page merge
	 * s2: Fast patch check
	 * s2: Insert
	 * s1: Insert
	 *
	 * Due to page merge, we might end up with double insert.  This even fast
	 * path check requires unique lwlock.
	 */
	if (fastpath && LWLockConditionalAcquire(uniqueLock, LW_EXCLUSIVE))
	{
		OTupleXactInfo xactInfo;

		if (page_unique_check(desc, p, &pageFindContext.items[pageFindContext.index].locator,
							  key, opOxid, &xactInfo))
		{
			OTuple		curTuple;
			BTreeLocationHint cbHint = {pageFindContext.items[pageFindContext.index].blkno, pageFindContext.items[pageFindContext.index].pageChangeCount};
			BTreeLeafTuphdr *tuphdr;

			BTREE_PAGE_READ_LEAF_ITEM(tuphdr, curTuple, p, &pageFindContext.items[pageFindContext.index].locator);

			if (XACT_INFO_OXID_EQ(xactInfo, opOxid) || XACT_INFO_IS_FINISHED(xactInfo))
			{
				OBTreeModifyCallbackAction cbAction PG_USED_FOR_ASSERTS_ONLY;

				if (callbackInfo->modifyCallback)
				{
					cbAction = callbackInfo->modifyCallback(desc,
															curTuple, &tuple, opOxid,
															xactInfo, tuphdr->undoLocation,
															&lockMode, &cbHint, callbackInfo->arg);

					/*
					 * We could support other callback actions, but it's not
					 * yet needed.
					 */
					Assert(cbAction == OBTreeCallbackActionDoNothing);
				}
				unlock_page(blkno);
				LWLockRelease(uniqueLock);
				return OBTreeModifyResultFound;
			}
			else
			{
				OBTreeWaitCallbackAction cbAction;

				LWLockRelease(uniqueLock);
				if (callbackInfo->waitCallback)
				{
					cbAction = callbackInfo->waitCallback(desc,
														  curTuple, &tuple, XACT_INFO_GET_OXID(xactInfo),
														  xactInfo, tuphdr->undoLocation,
														  &lockMode, &cbHint, callbackInfo->arg);
					Assert(cbAction != OBTreeCallbackActionXidNoWait);
					if (cbAction == OBTreeCallbackActionXidExit)
					{
						unlock_page(blkno);
						return OBTreeModifyResultFound;
					}
				}
				unlock_page(blkno);
				wait_for_oxid(XACT_INFO_GET_OXID(xactInfo), false);
				findResult = refind_page(&pageFindContext, key,
										 BTreeKeyUniqueLowerBound, 0,
										 blkno, pageChangeCount);
				Assert(findResult == OFindPageResultSuccess);
				goto retry;
			}
		}
		else
		{
			/*
			 * We've to find approprivate offset for the new tuple.  It should
			 * be within the page, but can not match current offset, because
			 * we've searched for BTreeUniqueMinBound.
			 */
			btree_page_search(desc, p, key, BTreeKeyBound,
							  NULL, &pageFindContext.items[pageFindContext.index].locator);
		}
	}
	else
	{
		OTupleXactInfo xactInfo;

		/*
		 * Evade deadlock: unlock the page before taking an unique lwlock.
		 */
		unlock_page(blkno);

		LWLockAcquire(uniqueLock, LW_EXCLUSIVE);

		if (slowpath_unique_check(desc, &pageFindContext, key,
								  opOxid, &xactInfo))
		{
			BTreePageItemLocator *loc = &pageFindContext.items[pageFindContext.index].locator;
			OTuple		curTuple;
			BTreeLocationHint cbHint = {pageFindContext.items[pageFindContext.index].blkno, pageFindContext.items[pageFindContext.index].pageChangeCount};
			BTreeLeafTuphdr *tuphdr;

			p = O_GET_IN_MEMORY_PAGE(pageFindContext.items[pageFindContext.index].blkno);
			BTREE_PAGE_READ_LEAF_ITEM(tuphdr, curTuple, p, loc);
			if (XACT_INFO_OXID_EQ(xactInfo, opOxid) || XACT_INFO_IS_FINISHED(xactInfo))
			{
				OBTreeModifyCallbackAction cbAction PG_USED_FOR_ASSERTS_ONLY;

				if (callbackInfo->modifyCallback)
				{
					cbAction = callbackInfo->modifyCallback(desc,
															curTuple, &tuple, opOxid,
															xactInfo, tuphdr->undoLocation,
															&lockMode, &cbHint, callbackInfo->arg);

					/*
					 * We could support other callback actions, but it's not
					 * yet needed.
					 */
					Assert(cbAction == OBTreeCallbackActionDoNothing);
				}
				LWLockRelease(uniqueLock);
				return OBTreeModifyResultFound;
			}
			else
			{
				OBTreeWaitCallbackAction cbAction;

				LWLockRelease(uniqueLock);

				if (callbackInfo->waitCallback)
				{
					cbAction = callbackInfo->waitCallback(desc,
														  curTuple, &tuple, XACT_INFO_GET_OXID(xactInfo),
														  tuphdr->undoLocation,
														  xactInfo, &lockMode, &cbHint, callbackInfo->arg);
					Assert(cbAction != OBTreeCallbackActionXidNoWait);
					if (cbAction == OBTreeCallbackActionXidExit)
					{
						return OBTreeModifyResultFound;
					}
				}
				wait_for_oxid(XACT_INFO_GET_OXID(xactInfo), false);
				BTREE_PAGE_FIND_SET(&pageFindContext, MODIFY);
				findResult = refind_page(&pageFindContext, key,
										 BTreeKeyUniqueLowerBound, 0,
										 blkno, pageChangeCount);
				Assert(findResult == OFindPageResultSuccess);
				goto retry;
			}
		}
		else
		{
			BTREE_PAGE_FIND_SET(&pageFindContext, MODIFY);
			findResult = find_page(&pageFindContext, key, BTreeKeyBound, 0);
			Assert(findResult == OFindPageResultSuccess);
		}
	}

	result = o_btree_modify_internal(&pageFindContext, BTreeOperationInsert,
									 tuple, tupleType, key,
									 keyType, opOxid, opCsn, lockMode,
									 BTreeLeafTupleNonDeleted, pageReserveKind,
									 callbackInfo);

	LWLockRelease(uniqueLock);
	return result;
}

OBTreeModifyResult
o_btree_modify(BTreeDescr *desc, BTreeOperationType action,
			   OTuple tuple, BTreeKeyType tupleType,
			   Pointer key, BTreeKeyType keyType,
			   OXid oxid, CommitSeqNo csn, RowLockMode lockMode,
			   BTreeLocationHint *hint, BTreeModifyCallbackInfo *callbackInfo)
{
	return o_btree_normal_modify(desc, action, tuple, tupleType,
								 key, keyType, oxid, csn, lockMode,
								 hint, BTreeLeafTupleNonDeleted, callbackInfo);
}

OBTreeModifyResult
o_btree_delete_moved_partitions(BTreeDescr *desc, Pointer key,
								BTreeKeyType keyType, OXid oxid,
								CommitSeqNo csn,
								BTreeLocationHint *hint,
								BTreeModifyCallbackInfo *callbackInfo)
{
	OTuple		nullTup;

	O_TUPLE_SET_NULL(nullTup);

	return o_btree_normal_modify(desc, BTreeOperationDelete,
								 nullTup, BTreeKeyNone,
								 key, keyType, oxid, csn, RowLockUpdate,
								 hint, BTreeLeafTupleMovedPartitions,
								 callbackInfo);
}

OBTreeModifyResult
o_btree_delete_pk_changed(BTreeDescr *desc, Pointer key,
						  BTreeKeyType keyType, OXid oxid,
						  CommitSeqNo csn,
						  BTreeLocationHint *hint,
						  BTreeModifyCallbackInfo *callbackInfo)
{
	OTuple		nullTup;

	O_TUPLE_SET_NULL(nullTup);

	return o_btree_normal_modify(desc, BTreeOperationDelete,
								 nullTup, BTreeKeyNone,
								 key, keyType, oxid, csn, RowLockUpdate,
								 hint, BTreeLeafTuplePKChanged,
								 callbackInfo);
}

bool
o_btree_autonomous_insert(BTreeDescr *desc, OTuple tuple)
{
	OAutonomousTxState state;
	OBTreeModifyResult result;

	if (desc->storageType == BTreeStoragePersistence)
	{
		start_autonomous_transaction(&state);
		PG_TRY();
		{
			result = o_btree_normal_modify(desc, BTreeOperationInsert,
										   tuple, BTreeKeyLeafTuple,
										   NULL, BTreeKeyNone,
										   get_current_oxid(),
										   COMMITSEQNO_INPROGRESS,
										   RowLockUpdate,
										   NULL, BTreeLeafTupleNonDeleted,
										   &nullCallbackInfo);
			o_wal_insert(desc, tuple);
		}
		PG_CATCH();
		{
			abort_autonomous_transaction(&state);
			PG_RE_THROW();
		}
		PG_END_TRY();
		finish_autonomous_transaction(&state);
	}
	else
	{
		result = o_btree_normal_modify(desc, BTreeOperationInsert,
									   tuple, BTreeKeyLeafTuple,
									   NULL, BTreeKeyNone,
									   InvalidOXid,
									   COMMITSEQNO_INPROGRESS,
									   RowLockUpdate,
									   NULL, BTreeLeafTupleNonDeleted,
									   &nullCallbackInfo);
	}

	return (result == OBTreeModifyResultInserted);
}

bool
o_btree_autonomous_delete(BTreeDescr *desc, OTuple key, BTreeKeyType keyType,
						  BTreeLocationHint *hint)
{
	OAutonomousTxState state;
	OBTreeModifyResult result;

	Assert(keyType == BTreeKeyLeafTuple || keyType == BTreeKeyNonLeafKey);

	if (desc->storageType == BTreeStoragePersistence)
	{
		start_autonomous_transaction(&state);
		PG_TRY();
		{
			result = o_btree_normal_modify(desc, BTreeOperationDelete,
										   key, keyType,
										   NULL, BTreeKeyNone,
										   get_current_oxid(), COMMITSEQNO_INPROGRESS,
										   RowLockUpdate,
										   hint, BTreeLeafTupleNonDeleted,
										   &nullCallbackInfo);
			if (keyType == BTreeKeyLeafTuple)
				o_wal_delete(desc, key);
			else if (keyType == BTreeKeyNonLeafKey)
				o_wal_delete_key(desc, key);
		}
		PG_CATCH();
		{
			abort_autonomous_transaction(&state);
			PG_RE_THROW();
		}
		PG_END_TRY();
		finish_autonomous_transaction(&state);
	}
	else
	{
		result = o_btree_normal_modify(desc, BTreeOperationDelete,
									   key, keyType,
									   NULL, BTreeKeyNone,
									   InvalidOXid, COMMITSEQNO_INPROGRESS,
									   RowLockUpdate,
									   hint, BTreeLeafTupleNonDeleted,
									   &nullCallbackInfo);
	}

	return (result == OBTreeModifyResultDeleted);
}
