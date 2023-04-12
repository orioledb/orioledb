/*-------------------------------------------------------------------------
 *
 * modify.h
 * 		Declarations for OrioleDB B-tree modification.
 *
 * Copyright (c) 2021-2022, Oriole DB Inc.
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
												BTreeModifyCallbackInfo *callbackInfo);

#endif							/* __BTREE_MODIFY_H__ */
