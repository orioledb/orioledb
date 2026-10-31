/*-------------------------------------------------------------------------
 *
 * modify.h
 * 		Declarations for OrioleDB B-tree modification.
 *
 * Copyright (c) 2021-2026, Oriole DB Inc.
 * Copyright (c) 2025-2026, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/btree/modify.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __BTREE_MODIFY_H__
#define __BTREE_MODIFY_H__

#include "btree.h"

typedef struct BTreeModifyCallbackInfo
{
	OBTreeWaitCallbackAction (*waitCallback) (BTreeDescr *desc,
											  OTuple oldTup,
											  OTuple *newTup,
											  OXid oxid,
											  OTupleXactInfo prevXactInfo,
											  UndoLocation location,
											  RowLockMode *lockMode,
											  BTreeLocationHint *hint,
											  void *arg);
	OBTreeModifyCallbackAction (*modifyCallback) (BTreeDescr *desc,
												  OTuple oldTup,
												  OTuple *newTup,
												  OXid oxid,
												  OTupleXactInfo prevXactInfo,
												  UndoLocation location,
												  RowLockMode *lockMode,
												  BTreeLocationHint *hint,
												  void *arg);
	OBTreeModifyCallbackAction (*modifyDeletedCallback) (BTreeDescr *desc,
														 OTuple oldTup,
														 OTuple *newTup,
														 OXid oxid,
														 OTupleXactInfo prevXactInfo,
														 BTreeLeafTupleDeletedStatus deleted,
														 UndoLocation location,
														 RowLockMode *lockMode,
														 BTreeLocationHint *hint,
														 void *arg);
	bool		needsUndoForSelfCreated;
	void	   *arg;

	/*
	 * Optional hook fired once per successful PK-side modification, while the
	 * affected leaf page is still locked.  Called with the freshly created
	 * undo location (real value from make_undo_record), or with the
	 * WaitingSkUndoLoc sentinel when the self-created shortcut skips undo
	 * entirely.  Used by the table AM to install the PK-applied/SK-pending
	 * marker before the page lock drops, eliminating the race window that
	 * would exist if the marker was written by the caller after
	 * o_btree_modify() returned.  The same `arg` is passed as the other
	 * callbacks above receive; the hook is expected to extract the
	 * OTableDescr from whatever arg type the caller chose.
	 */
	void		(*postUndoRecorded) (UndoLocation undoLoc, void *arg);

	/*
	 * Identifies the delegated form of the two hooks above, for operations
	 * the holder of the leaf page is allowed to finish on our behalf.  Zero
	 * means there is none and the operation cannot be delegated.  See
	 * btree_register_delegated_callbacks().
	 */
	int			delegatedCallbackId;
} BTreeModifyCallbackInfo;

/*
 * What a delegated callback hands back to the process it acted for.  The
 * holder cannot touch the waiter's memory, so anything the waiter needs to
 * learn about the row it replaced travels here and is applied once it wakes.
 */
typedef struct BTreeDelegatedModifyResult
{
	OTupleXactInfo xactInfo;	/* of the row as we found it */
	UndoLocation undoLocation;	/* its undo location */
	bool		deleted;		/* was it a deleted row? */
} BTreeDelegatedModifyResult;

/*
 * Delegated form of modifyCallback, run by whoever holds the page rather than
 * by the process the work belongs to.
 *
 * It gets everything explicitly, because none of the usual process-local
 * state means what it normally would: `oxid` is the waiting transaction's,
 * not ours, and there is no `arg` at all, since that is a pointer into
 * another backend.  It may read the row and amend the new tuple in place --
 * that tuple lives in shared memory and its owner is asleep -- but it must
 * not change the tuple's length, and must leave for the waiter, in *result,
 * whatever the waiter has to record for itself.
 *
 * Returning anything but OBTreeCallbackActionUpdate declines the delegation:
 * the waiter is woken and does the whole operation itself.
 */
typedef OBTreeModifyCallbackAction (*BTreeDelegatedModifyCallback)
			(BTreeDescr *desc, OTuple curTuple, OTuple *newTuple, OXid oxid,
			 OTupleXactInfo xactInfo, UndoLocation location,
			 RowLockMode *lockMode, BTreeDelegatedModifyResult *result);

/*
 * Delegated form of postUndoRecorded.  Runs in the holder, so it is told
 * which process it is acting for instead of using its own.
 */
typedef void (*BTreeDelegatedPostUndoCallback) (BTreeDescr *desc,
												UndoLocation undoLoc,
												int pgprocno);

/*
 * Run by the waiter itself once it wakes, on its own `arg`, to record what the
 * holder found.  This is the half of the old callback that touches memory only
 * the waiting process has.
 */
typedef void (*BTreeDelegatedApplyResultCallback) (BTreeDescr *desc,
												   BTreeDelegatedModifyResult *result,
												   void *arg);

extern int	btree_register_delegated_callbacks(BTreeDelegatedModifyCallback modifyCallback,
											   BTreeDelegatedPostUndoCallback postUndoCallback,
											   BTreeDelegatedApplyResultCallback applyResultCallback);
extern BTreeDelegatedModifyCallback btree_get_delegated_modify_callback(int id);
extern BTreeDelegatedPostUndoCallback btree_get_delegated_post_undo_callback(int id);
extern BTreeDelegatedApplyResultCallback btree_get_delegated_apply_result_callback(int id);

extern BTreeModifyCallbackInfo nullCallbackInfo;
extern void btree_apply_waiter_ops(BTreeDescr *desc, OInMemoryBlkno blkno);

extern bool o_btree_autonomous_insert(BTreeDescr *desc, OTuple tuple);
extern bool o_btree_autonomous_delete(BTreeDescr *desc, OTuple key, BTreeKeyType keyType,
									  BTreeLocationHint *hint);
extern OBTreeModifyResult o_btree_modify(BTreeDescr *desc,
										 BTreeOperationType action,
										 OTuple tuple,
										 BTreeKeyType tupleType,
										 Pointer key,
										 BTreeKeyType keyType,
										 OXid oxid, CommitSeqNo csn,
										 RowLockMode lockMode,
										 BTreeLocationHint *hint,
										 BTreeModifyCallbackInfo *callbackInfo);
extern OBTreeModifyResult o_btree_delete_moved_partitions(BTreeDescr *desc,
														  Pointer key,
														  BTreeKeyType keyType,
														  OXid oxid, CommitSeqNo csn,
														  BTreeLocationHint *hint,
														  BTreeModifyCallbackInfo *callbackInfo);
extern OBTreeModifyResult o_btree_delete_pk_changed(BTreeDescr *desc,
													Pointer key,
													BTreeKeyType keyType,
													OXid oxid, CommitSeqNo csn,
													BTreeLocationHint *hint,
													BTreeModifyCallbackInfo *callbackInfo);
extern OBTreeModifyResult o_btree_insert_unique(BTreeDescr *desc,
												OTuple tuple,
												BTreeKeyType tupleType,
												Pointer key,
												BTreeKeyType keyType,
												OXid my_oxid, CommitSeqNo my_csn,
												RowLockMode lock_mode,
												BTreeLocationHint *hint,
												BTreeModifyCallbackInfo *callbackInfo,
												IndexUniqueCheck checkUnique);

#endif							/* __BTREE_MODIFY_H__ */
