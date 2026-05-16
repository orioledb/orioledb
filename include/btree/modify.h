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
} BTreeModifyCallbackInfo;

extern BTreeModifyCallbackInfo nullCallbackInfo;

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
