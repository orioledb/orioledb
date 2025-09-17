/*-------------------------------------------------------------------------
 *
 * operations.h
 *		Declarations of table-level operations
 *
 * Copyright (c) 2021-2025, Oriole DB Inc.
 * Copyright (c) 2025, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/tableam/operations.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __TABLEAM_OPERATIONS_H__
#define __TABLEAM_OPERATIONS_H__

#include "btree/btree.h"
#include "btree/modify.h"
#include "c.h"
#include "catalog/o_tables.h"
#include "tableam/descr.h"
#include "tuple/slot.h"

#include "access/tableam.h"
#include "nodes/execnodes.h"
#include "nodes/pathnodes.h"
#include "rewrite/rewriteHandler.h"

/*
 * Result of table modification functions.
 */
typedef struct OTableModifyResult
{
	/* result of the modification */
	bool		success;
	/* a failed modification action */
	BTreeOperationType action;
	/* an index number on which the modification action has been failed */
	OIndexNumber failedIxNum;
	/* the modified tuple */
	TupleTableSlot *oldTuple;
} OTableModifyResult;

typedef struct
{
	OTableDescr *desc;
	TupleTableSlot *scanSlot;
	OTableSlot *newSlot;
	OXid		conflictOxid;
	OXid		oxid;
	CommitSeqNo csn;
	UndoLocation tupUndoLocation;
	OIndexNumber conflictIxNum;
	bool		copyPrimaryOxid;
	RowLockMode lockMode;
} InsertOnConflictCallbackArg;

typedef struct
{
	TupleTableSlot *scanSlot;
	TupleTableSlot *tmpSlot;
	OTableDescr *descr;
	OTableSlot *newSlot;
	OXid		oxid;
	CommitSeqNo csn;
	UndoLocation tup_undo_location;
	BTreeLeafTupleDeletedStatus deleted;
	CommandId	modifyCid;
	CommandId	tupleCid;
	bool		modified;
	bool		selfModified;
	bool		changingPart;
	Bitmapset  *keyAttrs;
	int			options;
} OModifyCallbackArg;

typedef struct
{
	Relation	rel;
	TupleTableSlot *scanSlot;
	OTableDescr *descr;
	OXid		oxid;
	CommitSeqNo csn;
	LockWaitPolicy waitPolicy;
	UndoLocation tupUndoLocation;
	BTreeLeafTupleDeletedStatus deleted;
	CommandId	modifyCid;
	CommandId	tupleCid;
	bool		wouldBlock;
	bool		modified;
	bool		selfModified;
} OLockCallbackArg;

extern TupleTableSlot *o_tbl_insert(OTableDescr *descr, Relation relation,
									TupleTableSlot *slot, OXid oxid,
									CommitSeqNo csn);
extern TupleTableSlot *o_tbl_insert_with_arbiter(Relation rel,
												 OTableDescr *descr,
												 TupleTableSlot *slot,
												 List *arbiterIndexes,
												 CommandId cid,
												 LockTupleMode lockmode,
												 TupleTableSlot *lockedSlot);
extern OBTreeModifyResult o_tbl_index_insert(OTableDescr *descr,
											 OIndexDescr *id,
											 OTuple *own_tup,
											 TupleTableSlot *slot,
											 OXid oxid, CommitSeqNo csn,
											 BTreeModifyCallbackInfo *callbackInfo);
extern OBTreeModifyResult o_tbl_lock(OTableDescr *descr, OBTreeKeyBound *pkey,
									 LockTupleMode mode, OXid oxid,
									 OLockCallbackArg *larg,
									 BTreeLocationHint *hint);
extern OTableModifyResult o_tbl_update(OTableDescr *descr, TupleTableSlot *slot,
									   OBTreeKeyBound *oldPkey,
									   Relation rel, OXid oxid,
									   CommitSeqNo csn,
									   BTreeLocationHint *hint,
									   OModifyCallbackArg *arg,
									   ItemPointer bridge_ctid);
extern OTableModifyResult o_update_secondary_index(OIndexDescr *id,
												   OIndexNumber ix_num,
												   bool new_valid,
												   bool old_valid,
												   TupleTableSlot *newSlot,
												   OTuple new_ix_tup,
												   TupleTableSlot *oldSlot,
												   OXid oxid,
												   CommitSeqNo csn);
extern OTableModifyResult o_tbl_delete(Relation rel,
									   OTableDescr *descr,
									   OBTreeKeyBound *primary_key,
									   OXid oxid,
									   CommitSeqNo csn,
									   BTreeLocationHint *hint,
									   OModifyCallbackArg *arg);
extern OTableModifyResult o_tbl_index_delete(OIndexDescr *id,
											 OIndexNumber ix_num,
											 TupleTableSlot *slot,
											 OXid oxid, CommitSeqNo csn);
extern void o_check_tbl_update_mres(OTableModifyResult mres,
									OTableDescr *descr,
									Relation rel,
									TupleTableSlot *slot);
extern void o_check_tbl_delete_mres(OTableModifyResult mres,
									OTableDescr *descr, Relation rel);

extern bool o_is_index_predicate_satisfied(OIndexDescr *idx,
										   TupleTableSlot *slot,
										   ExprContext *econtext);
extern void o_truncate_table(ORelOids oids);

#endif
