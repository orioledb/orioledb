/*-------------------------------------------------------------------------
 *
 * operations.c
 *		Implementation of table-level operations
 *
 * Copyright (c) 2021-2022, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/tableam/operations.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "orioledb.h"

#include "btree/btree.h"
#include "btree/iterator.h"
#include "btree/modify.h"
#include "recovery/recovery.h"
#include "recovery/wal.h"
#include "tableam/descr.h"
#include "tableam/handler.h"
#include "tableam/operations.h"
#include "tableam/tree.h"
#include "transam/oxid.h"
#include "transam/undo.h"
#include "tuple/slot.h"
#include "utils/stopevent.h"

#include "access/heapam.h"
#include "access/tableam.h"
#include "catalog/index.h"
#include "catalog/storage.h"
#include "commands/vacuum.h"
#include "nodes/execnodes.h"
#include "parser/parsetree.h"
#include "storage/bufmgr.h"
#include "utils/lsyscache.h"

static OTableModifyResult o_update_secondary_indices(OTableDescr *descr,
													 TupleTableSlot *newSlot,
													 TupleTableSlot *oldSlot,
													 EState *estate,
													 OXid oxid,
													 CommitSeqNo csn,
													 bool update_all);

static OTableModifyResult o_tbl_indices_insert(TupleTableSlot *slot,
											   OTableDescr *descr,
											   EState *estate,
											   OIndexNumber start_ix, OXid oxid,
											   CommitSeqNo csn,
											   BTreeModifyCallbackInfo *callbackInfo);
static OTableModifyResult o_tbl_indices_overwrite(OTableDescr *descr,
												  OBTreeKeyBound *oldPkey,
												  TupleTableSlot *newSlot,
												  EState *estate,
												  OXid oxid, CommitSeqNo csn,
												  BTreeLocationHint *hint,
												  OModifyCallbackArg *arg);
static OTableModifyResult o_tbl_indices_reinsert(OTableDescr *descr,
												 OBTreeKeyBound *oldPkey,
												 OBTreeKeyBound *newPkey,
												 TupleTableSlot *newSlot,
												 EState *estate,
												 OXid oxid, CommitSeqNo csn,
												 BTreeLocationHint *hint,
												 OModifyCallbackArg *arg);
static OTableModifyResult o_tbl_indices_delete(OTableDescr *descr,
											   OBTreeKeyBound *key, EState *estate,
											   OXid oxid, CommitSeqNo csn,
											   BTreeLocationHint *hint,
											   OModifyCallbackArg *arg);
static void o_toast_insert_values(Relation rel, OTableDescr *descr,
								  TupleTableSlot *slot, OXid oxid, CommitSeqNo csn);
static inline bool o_callback_is_modified(OXid oxid, CommitSeqNo csn, OTupleXactInfo xactInfo);
static OBTreeModifyCallbackAction o_insert_callback(BTreeDescr *descr,
													OTuple tup, OTuple *newtup,
													OXid oxid, OTupleXactInfo xactInfo,
													RowLockMode *lock_mode,
													BTreeLocationHint *hint,
													void *arg);
static OBTreeWaitCallbackAction o_insert_on_conflict_wait_callback(BTreeDescr *descr,
																   OTuple tup, OTuple *newtup,
																   OXid oxid, OTupleXactInfo xactInfo,
																   RowLockMode *lock_mode,
																   BTreeLocationHint *hint,
																   void *arg);
static OBTreeModifyCallbackAction o_insert_on_conflict_insert_to_deleted_callback(BTreeDescr *descr,
																				  OTuple tup, OTuple *newtup,
																				  OXid oxid, OTupleXactInfo xactInfo,
																				  RowLockMode *lock_mode,
																				  BTreeLocationHint *hint,
																				  void *arg);
static OBTreeModifyCallbackAction o_insert_on_conflict_modify_callback(BTreeDescr *descr,
																	   OTuple tup, OTuple *newtup,
																	   OXid oxid, OTupleXactInfo xactInfo,
																	   RowLockMode *lock_mode,
																	   BTreeLocationHint *hint,
																	   void *arg);
static OBTreeModifyCallbackAction o_delete_callback(BTreeDescr *descr,
													OTuple tup, OTuple *newtup,
													OXid oxid, OTupleXactInfo xactInfo,
													RowLockMode *lock_mode,
													BTreeLocationHint *hint,
													void *arg);
static OBTreeModifyCallbackAction o_update_callback(BTreeDescr *descr,
													OTuple tup, OTuple *newtup,
													OXid oxid, OTupleXactInfo xactInfo,
													RowLockMode *lock_mode,
													BTreeLocationHint *hint,
													void *arg);
static OBTreeWaitCallbackAction o_lock_wait_callback(BTreeDescr *descr, OTuple tup, OTuple *newtup,
													 OXid oxid, OTupleXactInfo xactInfo,
													 RowLockMode *lock_mode, BTreeLocationHint *hint,
													 void *arg);
static OBTreeModifyCallbackAction o_lock_modify_callback(BTreeDescr *descr, OTuple tup, OTuple *newtup,
														 OXid oxid, OTupleXactInfo xactInfo,
														 RowLockMode *lock_mode, BTreeLocationHint *hint,
														 void *arg);
static void get_keys_from_ps(TupleTableSlot *planSlot, OIndexDescr *id,
							 int *junkAttrs, OBTreeKeyBound *key);
static inline bool is_keys_eq(BTreeDescr *desc, OBTreeKeyBound *k1, OBTreeKeyBound *k2);
static void o_report_duplicate(Relation rel, OIndexDescr *id,
							   TupleTableSlot *slot);

void
o_tbl_insert(OTableDescr *descr, Relation relation,
			 EState *estate, TupleTableSlot *slot,
			 OXid oxid, CommitSeqNo csn)
{
	OTableModifyResult mres;
	OTuple		tup;
	OIndexDescr *primary = GET_PRIMARY(descr);
	BTreeModifyCallbackInfo callbackInfo =
	{
		.waitCallback = NULL,
		.insertToDeleted = o_insert_callback,
		.modifyCallback = NULL,
		.needsUndoForSelfCreated = false,
		.arg = slot
	};

	if (slot->tts_ops != descr->newTuple->tts_ops)
	{
		ExecCopySlot(descr->newTuple, slot);
		slot = descr->newTuple;
	}

	if (GET_PRIMARY(descr)->primaryIsCtid)
	{
		o_btree_load_shmem(&primary->desc);
		slot->tts_tid = btree_ctid_get_and_inc(&primary->desc);
	}

	tts_orioledb_toast(slot, descr);

	tup = tts_orioledb_form_tuple(slot, descr);
	o_btree_check_size_of_tuple(o_tuple_size(tup, &primary->leafSpec),
								RelationGetRelationName(relation),
								false);

	mres = o_tbl_indices_insert(slot, descr, estate, PrimaryIndexNumber, oxid,
								csn, &callbackInfo);

	if (!mres.success)
	{
		o_report_duplicate(relation, descr->indices[mres.failedIxNum], slot);
	}

	o_toast_insert_values(relation, descr, slot, oxid, csn);

	o_wal_insert(&primary->desc, tup);
}

OBTreeModifyResult
o_tbl_lock(OTableDescr *descr, OBTreeKeyBound *pkey, LockTupleMode mode,
		   OXid oxid, OLockCallbackArg *larg, BTreeLocationHint *hint)
{
	RowLockMode lock_mode;
	OBTreeModifyResult res;
	OTuple		nullTup;
	BTreeModifyCallbackInfo callbackInfo = {
		.waitCallback = o_lock_wait_callback,
		.insertToDeleted = NULL,
		.modifyCallback = o_lock_modify_callback,
		.needsUndoForSelfCreated = false,
		.arg = larg
	};

	o_btree_load_shmem(&GET_PRIMARY(descr)->desc);

	switch (mode)
	{
		case LockTupleKeyShare:
			lock_mode = RowLockKeyShare;
			break;
		case LockTupleShare:
			lock_mode = RowLockShare;
			break;
		case LockTupleNoKeyExclusive:
			lock_mode = RowLockNoKeyUpdate;
			break;
		case LockTupleExclusive:
			lock_mode = RowLockUpdate;
			break;
		default:
			elog(ERROR, "Unknown lock mode: %u", mode);
			break;
	}

	O_TUPLE_SET_NULL(nullTup);
	res = o_btree_modify(&GET_PRIMARY(descr)->desc, BTreeOperationLock,
						 nullTup, BTreeKeyNone, (Pointer) pkey, BTreeKeyBound,
						 oxid, larg->csn, lock_mode,
						 hint, &callbackInfo);

	Assert(res == OBTreeModifyResultLocked || res == OBTreeModifyResultFound || res == OBTreeModifyResultNotFound);

	return res;
}

TupleTableSlot *
o_tbl_insert_on_conflict(ModifyTableState *mstate,
						 EState *estate,
						 ResultRelInfo *rinfo,
						 OTableDescr *descr,
						 TupleTableSlot *slot,
						 OnConflictAction on_conflict,
						 OIndexNumber conflict_ix)
{
	OModifyCallbackArg marg;
	InsertOnConflictCallbackArg ioc_arg;
	OTableModifyResult mres;
	TupleTableSlot *scan_slot,
			   *confl_slot;
	UndoStackLocations undoStackLocations;
	OTuple		tup;
	CommitSeqNo csn;
	OXid		oxid;
	Relation	rel = rinfo->ri_RelationDesc;
	bool		on_update = on_conflict == ONCONFLICT_UPDATE,
				ignore_all = conflict_ix == InvalidIndexNumber;
	OBTreeKeyBound old_pkey;

	if (ignore_all)
	{
		Assert(!on_update);
		conflict_ix = PrimaryIndexNumber;
	}

	fill_current_oxid_csn(&oxid, &csn);
	undoStackLocations = get_cur_undo_locations();

	if (on_update)
		ioc_arg.scanSlot = rinfo->ri_onConflict->oc_Existing;
	else
		ioc_arg.scanSlot = NULL;
	ioc_arg.desc = descr;
	ioc_arg.conflictBTree = &descr->indices[conflict_ix]->desc;
	ioc_arg.conflictIxNum = conflict_ix;
	ioc_arg.copyPrimaryOxid = conflict_ix != PrimaryIndexNumber;
	ioc_arg.oxid = oxid;
	ioc_arg.newSlot = (OTableSlot *) slot;

	while (true)
	{
		CommitSeqNo save_csn = csn;
		OTuple		primary_tup;
		BTreeModifyCallbackInfo callbackInfo = {
			.waitCallback = o_insert_on_conflict_wait_callback,
			.insertToDeleted = o_insert_on_conflict_insert_to_deleted_callback,
			.modifyCallback = o_insert_on_conflict_modify_callback,
			.needsUndoForSelfCreated = true,
			.arg = &ioc_arg
		};

		if (ioc_arg.scanSlot)
			ExecClearTuple(ioc_arg.scanSlot);
		ioc_arg.conflictOxid = InvalidOXid;
		ioc_arg.csn = csn;

		memset(&mres, 0, sizeof(OTableModifyResult));

		mres = o_tbl_indices_insert(slot, descr, estate, conflict_ix, oxid,
									csn, &callbackInfo);

		if (mres.success)
		{
			BTreeDescr *primary = &GET_PRIMARY(descr)->desc;

			/* all OK */
			tts_orioledb_insert_toast_values(slot, descr, oxid, csn);

			tup = tts_orioledb_form_tuple(slot, descr);
			o_wal_insert(primary, tup);
			return slot;
		}

		/* failed to insert */
		release_undo_size(UndoReserveTxn);
		apply_undo_stack(oxid, &undoStackLocations, true);
		if (ioc_arg.conflictOxid != InvalidOXid)
		{
			/* helps avoid deadlocks */
			oxid_notify(ioc_arg.conflictOxid);
			(void) wait_for_oxid(ioc_arg.conflictOxid);
			continue;
		}

		if (!ignore_all && mres.failedIxNum != conflict_ix)
		{
			o_report_duplicate(rel, descr->indices[mres.failedIxNum], slot);
		}

		csn = ioc_arg.csn;

		if (on_update)
		{
			ExprContext *econtext = mstate->ps.ps_ExprContext;

			scan_slot = rinfo->ri_onConflict->oc_Existing;

			Assert(!ignore_all && scan_slot);

			if (conflict_ix != PrimaryIndexNumber)
			{
				OIndexDescr *primary_td = GET_PRIMARY(descr),
						   *conflict_td = descr->indices[conflict_ix];
				OBTreeKeyBound key;
				CommitSeqNo primaryTupleCsn;
				OTuple		sstup;
				BTreeLocationHint hint = {OInvalidInMemoryBlkno, 0};

				Assert(ioc_arg.scanSlot);

				tts_orioledb_fill_key_bound(scan_slot,
											primary_td,
											&key);
				o_btree_load_shmem(&primary_td->desc);
				primary_tup = o_btree_find_tuple_by_key(&primary_td->desc,
														(Pointer) &key, BTreeKeyBound,
														csn, &primaryTupleCsn,
														scan_slot->tts_mcxt, &hint);
				if (O_TUPLE_IS_NULL(primary_tup))
				{
					/* concurrent modify happens */
					csn = save_csn;
					continue;
				}

				o_fill_secondary_key_bound(&primary_td->desc,
										   &conflict_td->desc,
										   primary_tup, &key);
				sstup = ((OTableSlot *) scan_slot)->tuple;
				if (o_idx_cmp(&conflict_td->desc,
							  &sstup, BTreeKeyLeafTuple,
							  (Pointer) &key, BTreeKeyBound) != 0)
				{
					/* secondary key on primary tuple has been updated */
					csn = save_csn;
					pfree(primary_tup.data);
					continue;
				}

				tts_orioledb_store_tuple(scan_slot, primary_tup, descr,
										 primaryTupleCsn, PrimaryIndexNumber,
										 true, &hint);
			}

			Assert(!TTS_EMPTY(scan_slot));

			econtext->ecxt_scantuple = scan_slot;
			econtext->ecxt_innertuple = slot;
			econtext->ecxt_outertuple = NULL;

			if (!ExecQual(rinfo->ri_onConflict->oc_WhereClause, econtext))
			{
				return NULL;
			}

			ExecProject(rinfo->ri_onConflict->oc_ProjInfo);
			confl_slot = rinfo->ri_onConflict->oc_ProjSlot;

			ExecMaterializeSlot(confl_slot);

			STOPEVENT(STOPEVENT_IOC_BEFORE_UPDATE, NULL);

			marg.descr = descr;
			marg.oxid = oxid;
			marg.csn = csn;
			marg.rinfo = rinfo;
			marg.epqstate = NULL;
			marg.scanSlot = scan_slot;
			marg.modified = false;
			marg.rowLockMode = RowLockUpdate;
			marg.newSlot = (OTableSlot *) confl_slot;

			o_check_constraints(rinfo, confl_slot, estate);

			get_keys_from_ps(scan_slot, GET_PRIMARY(descr),
							 NULL, &old_pkey);

			mres = o_tbl_update(descr, confl_slot, estate, &old_pkey,
								rel, oxid, csn,
								&((OTableSlot *) scan_slot)->hint,
								&marg);

			if (!mres.success && marg.modified)
			{
				csn = save_csn;
				continue;
			}

			o_check_tbl_update_mres(mres, descr, rel, confl_slot);

			Assert(mres.success);

			if (mres.oldTuple == NULL)
			{
				/* primary key can be changed by concurrent transaction */
				csn = save_csn;
				continue;
			}

			return mres.oldTuple;
		}
		else
		{
			/* DO NOTHING; */
			return NULL;
		}
		Assert(false);
		return NULL;
	}

	Assert(false);
	return NULL;
}

OTableModifyResult
o_tbl_update(OTableDescr *descr, TupleTableSlot *slot, EState *estate,
			 OBTreeKeyBound *oldPkey, Relation rel, OXid oxid,
			 CommitSeqNo csn, BTreeLocationHint *hint,
			 OModifyCallbackArg *arg)
{
	TupleTableSlot *oldSlot;
	OTableModifyResult mres;
	OBTreeKeyBound newPkey;
	OTuple		newTup;
	OIndexDescr *primary = GET_PRIMARY(descr);

	if (primary->primaryIsCtid)
	{
		Assert(oldPkey->nkeys == 1);
		Assert(DatumGetPointer(oldPkey->keys[0].value));
		slot->tts_tid = *((ItemPointerData *) DatumGetPointer(oldPkey->keys[0].value));
	}

	tts_orioledb_toast(slot, descr);
	newTup = tts_orioledb_form_tuple(slot, descr);
	o_btree_check_size_of_tuple(o_tuple_size(newTup, &primary->leafSpec),
								RelationGetRelationName(rel),
								false);

	tts_orioledb_fill_key_bound(slot, GET_PRIMARY(descr), &newPkey);

	if (is_keys_eq(&GET_PRIMARY(descr)->desc, oldPkey, &newPkey))
	{
		mres = o_tbl_indices_overwrite(descr, oldPkey, slot, estate, oxid, csn,
									   hint, arg);
	}
	else
	{
		mres = o_tbl_indices_reinsert(descr, oldPkey, &newPkey, slot, estate,
									  oxid, csn, hint, arg);
	}
	csn = arg->csn;

	if (mres.success && mres.oldTuple != NULL)
	{
		if (mres.action == BTreeOperationUpdate)
		{
			oldSlot = mres.oldTuple;
			mres.failedIxNum = TOASTIndexNumber;
			mres.success = tts_orioledb_update_toast_values(oldSlot, slot, descr,
															oxid, csn);

			if (mres.success)
			{
				OTuple		final_tup = tts_orioledb_form_tuple(slot, descr);

				o_wal_update(&primary->desc, final_tup);
			}
		}
		else if (mres.action == BTreeOperationDelete)
		{
			oldSlot = mres.oldTuple;
			/* reinsert TOAST value */
			mres.failedIxNum = TOASTIndexNumber;
			/* insert new value in TOAST table */
			mres.success = tts_orioledb_insert_toast_values(slot, descr, oxid, csn);
			if (mres.success)
			{
				/* remove old value from TOAST table */
				mres.success = tts_orioledb_remove_toast_values(oldSlot, descr, oxid, csn);
			}

			if (mres.success)
			{
				OTuple		old_tup = ((OTableSlot *) oldSlot)->tuple,
							final_tup = tts_orioledb_form_tuple(slot, descr);

				o_wal_delete(&primary->desc, old_tup);
				o_wal_insert(&primary->desc, final_tup);
			}
		}
		else
		{
			Assert(mres.action == BTreeOperationLock);
			Assert(mres.oldTuple);
			return mres;
		}
	}

	if (mres.success && mres.oldTuple != NULL)
		mres.oldTuple = slot;

	return mres;
}

OTableModifyResult
o_tbl_delete(OTableDescr *descr, EState *estate, OBTreeKeyBound *primary_key,
			 OXid oxid, CommitSeqNo csn, BTreeLocationHint *hint,
			 OModifyCallbackArg *arg)
{
	OTableModifyResult result;

	result = o_tbl_indices_delete(descr, primary_key, estate, oxid,
								  csn, hint, arg);

	if (result.success && result.oldTuple != NULL)
	{
		if (result.action == BTreeOperationDelete)
		{
			OIndexDescr *primary = GET_PRIMARY(descr);
			OTuple		primary_tuple;

			csn = arg->csn;
			/* if tuple has been deleted from index trees, remove TOAST values */
			if (!tts_orioledb_remove_toast_values(arg->scanSlot, descr, oxid, csn))
			{
				result.success = false;
				result.failedIxNum = TOASTIndexNumber;
				return result;
			}

			primary_tuple = ((OTableSlot *) arg->scanSlot)->tuple;

			o_wal_delete(&primary->desc, primary_tuple);
		}
		else
		{
			Assert(result.action == BTreeOperationLock);
			return result;
		}
	}
	return result;
}

bool
o_is_index_predicate_satisfied(OIndexDescr *idx, TupleTableSlot *slot,
							   ExprContext *econtext)
{
	bool		result = true;

	/* Check for partial index */
	if (idx->predicate != NIL)
	{
		econtext->ecxt_scantuple = slot;
		/* Skip this index-update if the predicate isn't satisfied */
		if (!ExecQual(idx->predicate_state, econtext))
			result = false;
	}
	return result;
}

static OTableModifyResult
o_update_secondary_indices(OTableDescr *descr,
						   TupleTableSlot *newSlot,
						   TupleTableSlot *oldSlot,
						   EState *estate,
						   OXid oxid,
						   CommitSeqNo csn,
						   bool update_all)
{
	OTableModifyResult res;
	OBTreeKeyBound old_key,
				new_key;
	OIndexDescr *id;
	int			i;
	bool		update;
	OTuple		nullTup;
	ExprContext *econtext;
	BTreeModifyCallbackInfo callbackInfo = nullCallbackInfo;

	slot_getallattrs(oldSlot);
	res.success = true;
	res.oldTuple = oldSlot;

	econtext = GetPerTupleExprContext(estate);
	for (i = 1; i < descr->nIndices; i++)
	{
		bool		old_valid,
					new_valid;

		id = descr->indices[i];

		tts_orioledb_fill_key_bound(newSlot, id, &new_key);
		tts_orioledb_fill_key_bound(oldSlot, id, &old_key);

		old_valid = o_is_index_predicate_satisfied(id, oldSlot, econtext);
		new_valid = o_is_index_predicate_satisfied(id, newSlot, econtext);

		if (update_all)
			update = true;
		else
			update = !is_keys_eq(&id->desc, &old_key, &new_key) ||
				(old_valid != new_valid);

		if (update)
		{
			o_btree_load_shmem(&id->desc);
			O_TUPLE_SET_NULL(nullTup);

			if (old_valid)
				res.success = o_btree_modify(&id->desc, BTreeOperationDelete,
											 nullTup, BTreeKeyNone,
											 (Pointer) &old_key, BTreeKeyBound,
											 oxid, csn, RowLockUpdate,
											 NULL, &callbackInfo) == OBTreeModifyResultDeleted;
			else
				res.success = true;

			if (res.success && new_valid)
			{
				OTuple		new_ix_tup;

				new_ix_tup = tts_orioledb_make_secondary_tuple(newSlot, id, true);
				o_btree_check_size_of_tuple(o_tuple_size(new_ix_tup, &id->leafSpec),
											id->name.data,
											true);

				if (!id->unique || o_has_nulls(new_ix_tup))
					res.success = o_btree_modify(&id->desc, BTreeOperationInsert,
												 new_ix_tup, BTreeKeyLeafTuple,
												 (Pointer) &new_key, BTreeKeyBound,
												 oxid, csn, RowLockUpdate,
												 NULL, &callbackInfo) == OBTreeModifyResultInserted;
				else
					res.success = o_btree_insert_unique(&id->desc, new_ix_tup, BTreeKeyLeafTuple,
														(Pointer) &new_key, BTreeKeyBound,
														oxid, csn, RowLockUpdate,
														NULL, &callbackInfo) == OBTreeModifyResultInserted;

				if (res.success)
					continue;
				else
					res.action = BTreeOperationInsert;
			}
			else if (res.success)
			{
				continue;
			}
			else
			{
				res.action = BTreeOperationUpdate;
			}
			res.failedIxNum = i;
			break;
		}
	}
	return res;
}

/* returns TupleTableSlot of old tuple as OTableModifyResul.result */
static OTableModifyResult
o_tbl_indices_overwrite(OTableDescr *descr,
						OBTreeKeyBound *oldPkey,
						TupleTableSlot *newSlot,
						EState *estate,
						OXid oxid, CommitSeqNo csn,
						BTreeLocationHint *hint,
						OModifyCallbackArg *arg)
{
	OTableModifyResult result;
	TupleTableSlot *oldSlot;
	OTuple		newTup;
	OBTreeModifyResult modify_result;
	BTreeModifyCallbackInfo callbackInfo = {
		.waitCallback = NULL,
		.insertToDeleted = NULL,
		.modifyCallback = o_update_callback,
		.needsUndoForSelfCreated = false,
		.arg = arg
	};

	result.success = true;
	result.oldTuple = NULL;

	newTup = tts_orioledb_form_tuple(newSlot, descr);

	o_btree_load_shmem(&GET_PRIMARY(descr)->desc);

	modify_result = o_btree_modify(&GET_PRIMARY(descr)->desc, BTreeOperationUpdate,
								   newTup, BTreeKeyLeafTuple,
								   (Pointer) oldPkey, BTreeKeyBound,
								   oxid, csn, arg->rowLockMode,
								   hint, &callbackInfo);

	if (modify_result == OBTreeModifyResultLocked)
	{
		Assert(arg->scanSlot);
		result.success = true;
		result.oldTuple = arg->scanSlot;
		result.action = BTreeOperationLock;
		return result;
	}

	result.success = modify_result == OBTreeModifyResultUpdated;
	oldSlot = arg->scanSlot;
	csn = arg->csn;

	if (modify_result == OBTreeModifyResultUpdated)
	{
		((OTableSlot *) newSlot)->version = o_tuple_get_version(newTup);
		result = o_update_secondary_indices(descr, newSlot, oldSlot, estate,
											oxid, csn, false);
		if (result.success)
			result.action = BTreeOperationUpdate;
	}
	else if (modify_result == OBTreeModifyResultFound)
	{
		/* primary key or condition was changed by concurrent transaction */
		result.success = true;
		result.oldTuple = NULL;
	}
	else
	{
		result.oldTuple = NULL;
		result.action = BTreeOperationInsert;
		result.failedIxNum = PrimaryIndexNumber;
	}
	return result;
}

static OTableModifyResult
o_tbl_indices_reinsert(OTableDescr *descr,
					   OBTreeKeyBound *oldPkey,
					   OBTreeKeyBound *newPkey,
					   TupleTableSlot *newSlot,
					   EState *estate,
					   OXid oxid, CommitSeqNo csn,
					   BTreeLocationHint *hint, OModifyCallbackArg *arg)
{
	OTableModifyResult result;
	OBTreeModifyResult modify_result;
	OTuple		newTup;
	OTuple		nullTup;
	bool		inserted;
	BTreeModifyCallbackInfo deleteCallbackInfo = {
		.waitCallback = NULL,
		.insertToDeleted = NULL,
		.modifyCallback = o_delete_callback,
		.needsUndoForSelfCreated = false,
		.arg = arg
	};
	BTreeModifyCallbackInfo insertCallbackInfo = {
		.waitCallback = NULL,
		.insertToDeleted = o_insert_callback,
		.modifyCallback = NULL,
		.needsUndoForSelfCreated = false,
		.arg = newSlot
	};

	result.success = true;
	result.oldTuple = NULL;

	newTup = tts_orioledb_form_tuple(newSlot, descr);

	o_btree_load_shmem(&GET_PRIMARY(descr)->desc);

	O_TUPLE_SET_NULL(nullTup);
	modify_result = o_btree_modify(&GET_PRIMARY(descr)->desc, BTreeOperationDelete,
								   nullTup, BTreeKeyNone,
								   (Pointer) oldPkey, BTreeKeyBound,
								   oxid, csn, RowLockUpdate,
								   hint, &deleteCallbackInfo);

	if (modify_result == OBTreeModifyResultLocked)
	{
		Assert(arg->scanSlot);
		result.success = true;
		result.oldTuple = arg->scanSlot;
		result.action = BTreeOperationLock;
		return result;
	}
	else if (modify_result != OBTreeModifyResultDeleted)
	{
		result.success = false;
		result.action = BTreeOperationDelete;
		result.failedIxNum = PrimaryIndexNumber;
		return result;
	}

	inserted = o_btree_modify(&GET_PRIMARY(descr)->desc, BTreeOperationInsert,
							  newTup, BTreeKeyLeafTuple,
							  (Pointer) newPkey, BTreeKeyBound,
							  oxid, csn, RowLockUpdate,
							  NULL, &insertCallbackInfo) == OBTreeModifyResultInserted;
	((OTableSlot *) newSlot)->version = o_tuple_get_version(newTup);

	if (inserted)
	{
		result = o_update_secondary_indices(descr, newSlot, arg->scanSlot,
											estate, oxid, csn, true);
	}
	else
	{
		result.success = false;
		result.action = BTreeOperationInsert;
		result.failedIxNum = PrimaryIndexNumber;
	}

	if (result.success)
		result.action = BTreeOperationDelete;
	return result;
}

/* Returns TupleTableSlot of old tuple as OTableModifyResult.result */
static OTableModifyResult
o_tbl_indices_delete(OTableDescr *descr, OBTreeKeyBound *key, EState *estate,
					 OXid oxid, CommitSeqNo csn, BTreeLocationHint *hint,
					 OModifyCallbackArg *arg)
{
	OTableModifyResult result;
	OBTreeModifyResult res;
	TupleTableSlot *slot;
	OBTreeKeyBound bound;
	int			i;
	OTuple		nullTup;
	ExprContext *econtext;
	BTreeModifyCallbackInfo callbackInfo = {
		.waitCallback = NULL,
		.insertToDeleted = NULL,
		.modifyCallback = o_delete_callback,
		.needsUndoForSelfCreated = false,
		.arg = arg
	};

	result.oldTuple = NULL;

	o_btree_load_shmem(&GET_PRIMARY(descr)->desc);
	O_TUPLE_SET_NULL(nullTup);
	res = o_btree_modify(&GET_PRIMARY(descr)->desc, BTreeOperationDelete,
						 nullTup, BTreeKeyNone,
						 (Pointer) key, BTreeKeyBound,
						 oxid, csn, RowLockUpdate,
						 hint, &callbackInfo);

	slot = arg->scanSlot;
	csn = arg->csn;

	if (res == OBTreeModifyResultLocked)
	{
		result.success = true;
		result.oldTuple = slot;
		result.action = BTreeOperationLock;
		return result;
	}

	result.success = (res == OBTreeModifyResultDeleted);

	if (!result.success)
	{
		result.oldTuple = slot;
		result.failedIxNum = PrimaryIndexNumber;
		return result;
	}

	econtext = GetPerTupleExprContext(estate);
	/* removes from secondary indexes */
	for (i = 1; i < descr->nIndices; i++)
	{
		OIndexDescr *id = descr->indices[i];

		if ((i != PrimaryIndexNumber) &&
			!o_is_index_predicate_satisfied(id, slot, econtext))
			continue;

		tts_orioledb_fill_key_bound(slot, id, &bound);
		o_btree_load_shmem(&id->desc);
		res = o_btree_modify(&id->desc, BTreeOperationDelete,
							 nullTup, BTreeKeyNone,
							 (Pointer) &bound, BTreeKeyBound,
							 oxid, csn, RowLockUpdate,
							 NULL, &callbackInfo);

		result.success = (res == OBTreeModifyResultDeleted);
		if (!result.success)
		{
			result.success = false;
			result.failedIxNum = i;
			return result;
		}
	}

	result.success = true;
	result.action = BTreeOperationDelete;
	result.oldTuple = slot;
	return result;
}

static OTableModifyResult
o_tbl_indices_insert(TupleTableSlot *slot,
					 OTableDescr *descr,
					 EState *estate,
					 OIndexNumber start_ix,
					 OXid oxid, CommitSeqNo csn,
					 BTreeModifyCallbackInfo *callbackInfo)
{
	OTableModifyResult result;
	OTuple		tup;
	OBTreeKeyBound knew;
	bool		inserted = false;
	int			i;
	ExprContext *econtext;

	result.success = true;

	i = start_ix;
	Assert(i < descr->nIndices);

	econtext = GetPerTupleExprContext(estate);
	while (i < descr->nIndices)
	{
		OIndexDescr *id = descr->indices[i];
		BTreeDescr *bd = &id->desc;
		bool		primary = (i == PrimaryIndexNumber);
		bool		unique = descr->indices[i]->unique;
		bool		add_to_index = true;

		if (!primary && !o_is_index_predicate_satisfied(id, slot, econtext))
			add_to_index = false;

		if (add_to_index)
		{
			tts_orioledb_fill_key_bound(slot, descr->indices[i], &knew);

			if (!primary)
			{
				tup = tts_orioledb_make_secondary_tuple(slot, descr->indices[i], true);
				o_btree_check_size_of_tuple(o_tuple_size(tup, &id->leafSpec),
											descr->indices[i]->name.data,
											true);
			}
			else
			{
				tup = tts_orioledb_form_tuple(slot, descr);
			}

			o_btree_load_shmem(bd);
			if (primary || !unique || o_has_nulls(tup))
				inserted = o_btree_modify(bd, BTreeOperationInsert,
										  tup, BTreeKeyLeafTuple,
										  (Pointer) &knew, BTreeKeyBound,
										  oxid, csn, RowLockUpdate,
										  NULL, callbackInfo) == OBTreeModifyResultInserted;
			else
				inserted = o_btree_insert_unique(bd, tup, BTreeKeyLeafTuple,
												 (Pointer) &knew, BTreeKeyBound,
												 oxid, csn, RowLockUpdate,
												 NULL, callbackInfo) == OBTreeModifyResultInserted;

			((OTableSlot *) slot)->version = o_tuple_get_version(tup);

			if (!inserted)
			{
				result.success = false;
				result.failedIxNum = i;
				result.action = BTreeOperationInsert;
				result.oldTuple = NULL;
				break;
			}

			STOPEVENT(STOPEVENT_INDEX_INSERT, NULL);
		}

		if (i + 1 == start_ix)
			i += 2;
		else if (i == start_ix && start_ix != 0)
			i = 0;
		else
			i++;
	}

	return result;
}

static void
o_toast_insert_values(Relation rel, OTableDescr *descr,
					  TupleTableSlot *slot, OXid oxid, CommitSeqNo csn)
{
	if (!tts_orioledb_insert_toast_values(slot, descr, oxid, csn))
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Unable to insert TOASTable value in \"%s\"",
						RelationGetRelationName(rel)),
				 errdetail("Unable to remove value for primary key %s from TOAST",
						   tss_orioledb_print_idx_key(slot,
													  GET_PRIMARY(descr)))));
	}
}

void
o_check_tbl_update_mres(OTableModifyResult mres,
						OTableDescr *descr,
						Relation rel,
						TupleTableSlot *slot)
{
	if (!mres.success && mres.failedIxNum == TOASTIndexNumber)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Unable to update TOASTed value in \"%s\"",
						RelationGetRelationName(rel)),
				 errdetail("Unable to update value for primary key %s in TOAST",
						   tss_orioledb_print_idx_key(slot, GET_PRIMARY(descr)))));
	}

	if (!mres.success)
	{
		switch (mres.action)
		{
			case BTreeOperationUpdate:
				if (mres.failedIxNum == PrimaryIndexNumber)
					break;		/* it is ok */
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("unable to remove tuple from secondary index in \"%s\"",
								RelationGetRelationName(rel)),
						 errdetail("Unable to remove %s from index \"%s\"",
								   tss_orioledb_print_idx_key(slot, descr->indices[mres.failedIxNum]),
								   descr->indices[mres.failedIxNum]->name.data),
						 errtableconstraint(rel, "sk")));
				break;
			case BTreeOperationInsert:
				o_report_duplicate(rel, descr->indices[mres.failedIxNum], slot);
				break;
			default:
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("Unsupported BTreeOperationType.")));
				break;
		}
	}
}

void
o_check_tbl_delete_mres(OTableModifyResult mres,
						OTableDescr *descr,
						Relation rel)
{
	if (!mres.success && mres.failedIxNum == TOASTIndexNumber)
	{
		TupleTableSlot *oldSlot = mres.oldTuple;

		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Unable to remove value TOASTed value in \"%s\"",
						RelationGetRelationName(rel)),
				 errdetail("For primary key %s.",
						   tss_orioledb_print_idx_key(oldSlot,
													  GET_PRIMARY(descr)))));
	}

	if (!mres.success && mres.failedIxNum != PrimaryIndexNumber)
	{
		if (mres.oldTuple != NULL)
		{
			TupleTableSlot *oldSlot = mres.oldTuple;

			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("unable to remove tuple from secondary index in \"%s\"",
							RelationGetRelationName(rel)),
					 errdetail("Unable to remove %s from index %u",
							   tss_orioledb_print_idx_key(oldSlot,
														  GET_PRIMARY(descr)),
							   mres.failedIxNum),
					 errtableconstraint(rel, "sk")));
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Unable to remove tuple from secondary index in \"%s\"",
							RelationGetRelationName(rel)),
					 errdetail("Unable to fetch primary index table tuple.")));
		}
	}
}

/* returns true if tuple was changed by concurrent transaction. */
static inline bool
o_callback_is_modified(OXid oxid, CommitSeqNo csn, OTupleXactInfo xactInfo)
{
	if (XACT_INFO_OXID_EQ(xactInfo, oxid))
		return false;

	if (XACT_INFO_IS_FINISHED(xactInfo) && XACT_INFO_MAP_CSN(xactInfo) >= csn)
	{
		if (IsolationUsesXactSnapshot())
		{
			ereport(ERROR,
					(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
					 errmsg("%s", "could not serialize access due to concurrent update")));
		}
		return true;
	}
	return false;
}

static void
copy_tuple_to_slot(OTuple tup, TupleTableSlot *slot, OTableDescr *descr,
				   CommitSeqNo csn, OIndexNumber ix_num,
				   BTreeLocationHint *hint)
{
	OIndexDescr *id = descr->indices[ix_num];
	Size		sz = o_tuple_size(tup, &id->leafSpec);
	OTuple		copy;

	copy.data = (Pointer) MemoryContextAlloc(slot->tts_mcxt, sz);
	copy.formatFlags = tup.formatFlags;
	memcpy(copy.data, tup.data, sz);
	tts_orioledb_store_tuple(slot, copy, descr, csn, ix_num, true, hint);
}

static OBTreeModifyCallbackAction
o_insert_callback(BTreeDescr *descr, OTuple tup, OTuple *newtup,
				  OXid oxid, OTupleXactInfo xactInfo,
				  RowLockMode *lock_mode,
				  BTreeLocationHint *hint,
				  void *arg)
{
	OTableSlot *oslot = (OTableSlot *) arg;

	if (descr->type == oIndexPrimary &&
		XACT_INFO_OXID_IS_CURRENT(xactInfo))
	{
		OIndexDescr *id = (OIndexDescr *) descr->arg;

		o_tuple_set_version(&id->leafSpec, newtup,
							o_tuple_get_version(tup) + 1);
		oslot->tuple = *newtup;
	}
	return OBTreeCallbackActionUpdate;
}

static OBTreeWaitCallbackAction
o_insert_on_conflict_wait_callback(BTreeDescr *descr,
								   OTuple tup, OTuple *newtup,
								   OXid oxid, OTupleXactInfo xactInfo,
								   RowLockMode *lock_mode,
								   BTreeLocationHint *hint,
								   void *arg)
{
	InsertOnConflictCallbackArg *ioc_arg = (InsertOnConflictCallbackArg *) arg;
	BTreeDescr *td = ioc_arg->conflictBTree;
	OBTreeKeyBound new_sec_key,
				old_sec_key;

	ioc_arg->conflictOxid = InvalidOXid;
	if (descr->type != oIndexPrimary || !ioc_arg->copyPrimaryOxid)
		return OBTreeCallbackActionXidWait;

	o_fill_secondary_key_bound(descr, td, *newtup, &new_sec_key);
	o_fill_secondary_key_bound(descr, td, tup, &old_sec_key);

	if (o_idx_cmp(td, (Pointer) &new_sec_key, BTreeKeyBound,
				  (Pointer) &old_sec_key, BTreeKeyBound) == 0)
	{
		ioc_arg->conflictOxid = oxid;
		return OBTreeCallbackActionXidExit;
	}
	return OBTreeCallbackActionXidWait;
}

static OBTreeModifyCallbackAction
o_insert_on_conflict_insert_to_deleted_callback(BTreeDescr *descr,
												OTuple tup, OTuple *newtup,
												OXid oxid, OTupleXactInfo xactInfo,
												RowLockMode *lock_mode,
												BTreeLocationHint *hint,
												void *arg)
{
	InsertOnConflictCallbackArg *ioc_arg = (InsertOnConflictCallbackArg *) arg;

	if (descr->type == oIndexPrimary &&
		XACT_INFO_OXID_IS_CURRENT(xactInfo))
	{
		OIndexDescr *id = (OIndexDescr *) descr->arg;

		o_tuple_set_version(&id->leafSpec, newtup,
							o_tuple_get_version(tup) + 1);
		ioc_arg->newSlot->tuple = *newtup;
	}
	return OBTreeCallbackActionUpdate;
}

static OBTreeModifyCallbackAction
o_insert_on_conflict_modify_callback(BTreeDescr *descr,
									 OTuple tup, OTuple *newtup,
									 OXid oxid, OTupleXactInfo xactInfo,
									 RowLockMode *lock_mode,
									 BTreeLocationHint *hint,
									 void *arg)
{
	InsertOnConflictCallbackArg *ioc_arg = (InsertOnConflictCallbackArg *) arg;

	if (ioc_arg->scanSlot && descr == ioc_arg->conflictBTree)
	{
		bool		modified;

		modified = o_callback_is_modified(ioc_arg->oxid, ioc_arg->csn, xactInfo);

		/* Updates current csn */
		if (XACT_INFO_IS_FINISHED(xactInfo))
			ioc_arg->csn = modified ? (XACT_INFO_MAP_CSN(xactInfo) + 1) : ioc_arg->csn;
		else
			ioc_arg->csn = COMMITSEQNO_INPROGRESS;

		copy_tuple_to_slot(tup, ioc_arg->scanSlot, ioc_arg->desc,
						   ioc_arg->csn, ioc_arg->conflictIxNum, hint);
	}
	return OBTreeCallbackActionDoNothing;
}

static OBTreeModifyCallbackAction
o_delete_callback(BTreeDescr *descr,
				  OTuple tup, OTuple *newtup,
				  OXid oxid, OTupleXactInfo xactInfo, RowLockMode *lock_mode,
				  BTreeLocationHint *hint, void *arg)
{
	OModifyCallbackArg *o_arg = (OModifyCallbackArg *) arg;
	TupleTableSlot *inputslot;
	bool		modified;

	if (descr->type != oIndexPrimary)
		return OBTreeCallbackActionDelete;

	modified = o_callback_is_modified(o_arg->oxid, o_arg->csn, xactInfo);

	if (XACT_INFO_IS_FINISHED(xactInfo))
		o_arg->csn = modified ? (XACT_INFO_MAP_CSN(xactInfo) + 1) : o_arg->csn;
	else
		o_arg->csn = COMMITSEQNO_INPROGRESS;

	if (!o_arg->epqstate || !modified)
	{
		copy_tuple_to_slot(tup, o_arg->scanSlot, o_arg->descr, o_arg->csn,
						   PrimaryIndexNumber, hint);

		if (!modified)
		{
			return OBTreeCallbackActionDelete;
		}
		else
		{
			o_arg->modified = true;
			return OBTreeCallbackActionDoNothing;
		}
	}

	EvalPlanQualBegin(o_arg->epqstate);
	inputslot = EvalPlanQualSlot(o_arg->epqstate,
								 o_arg->rinfo->ri_RelationDesc,
								 o_arg->rinfo->ri_RangeTableIndex);
	o_arg->scanSlot = inputslot;
	copy_tuple_to_slot(tup, inputslot, o_arg->descr, o_arg->csn,
					   PrimaryIndexNumber, hint);

	return OBTreeCallbackActionLock;
}

static OBTreeModifyCallbackAction
o_update_callback(BTreeDescr *descr,
				  OTuple tup, OTuple *newtup,
				  OXid oxid, OTupleXactInfo xactInfo, RowLockMode *lock_mode,
				  BTreeLocationHint *hint, void *arg)
{
	OModifyCallbackArg *o_arg = (OModifyCallbackArg *) arg;
	TupleTableSlot *inputslot;
	bool		modified;
	uint32		version = 0;

	if (descr->type != oIndexPrimary)
		return OBTreeCallbackActionUpdate;

	if (descr->type == oIndexPrimary &&
		XACT_INFO_OXID_IS_CURRENT(xactInfo))
	{
		OIndexDescr *id = (OIndexDescr *) descr->arg;

		version = o_tuple_get_version(tup) + 1;
		o_tuple_set_version(&id->leafSpec, newtup, version);
		o_arg->newSlot->tuple = *newtup;
	}

	modified = o_callback_is_modified(o_arg->oxid, o_arg->csn, xactInfo);

	if (XACT_INFO_IS_FINISHED(xactInfo))
		o_arg->csn = modified ? (XACT_INFO_MAP_CSN(xactInfo) + 1) : o_arg->csn;
	else
		o_arg->csn = COMMITSEQNO_INPROGRESS;

	if (!o_arg->epqstate || !modified)
	{
		copy_tuple_to_slot(tup, o_arg->scanSlot, o_arg->descr, o_arg->csn,
						   PrimaryIndexNumber, hint);

		if (!modified)
		{
			return OBTreeCallbackActionUpdate;
		}
		else
		{
			o_arg->modified = true;
			return OBTreeCallbackActionDoNothing;
		}
	}

	EvalPlanQualBegin(o_arg->epqstate);
	inputslot = EvalPlanQualSlot(o_arg->epqstate,
								 o_arg->rinfo->ri_RelationDesc,
								 o_arg->rinfo->ri_RangeTableIndex);
	o_arg->scanSlot = inputslot;
	copy_tuple_to_slot(tup, inputslot, o_arg->descr, o_arg->csn,
					   PrimaryIndexNumber, hint);
	return OBTreeCallbackActionLock;
}

static OBTreeWaitCallbackAction
o_lock_wait_callback(BTreeDescr *descr, OTuple tup, OTuple *newtup,
					 OXid oxid, OTupleXactInfo xactInfo,
					 RowLockMode *lock_mode, BTreeLocationHint *hint,
					 void *arg)
{
	OLockCallbackArg *o_arg = (OLockCallbackArg *) arg;

	switch (o_arg->waitPolicy)
	{
		case LockWaitBlock:
			return OBTreeCallbackActionXidWait;
		case LockWaitSkip:
			o_arg->wouldBlock = true;
			return OBTreeCallbackActionXidExit;
		case LockWaitError:
			ereport(ERROR,
					(errcode(ERRCODE_LOCK_NOT_AVAILABLE),
					 errmsg("could not obtain lock on row in relation \"%s\"",
							RelationGetRelationName(o_arg->rel))));
			break;
		default:
			elog(ERROR, "Unknown wait policy: %u", o_arg->waitPolicy);
			break;
	}
}

static OBTreeModifyCallbackAction
o_lock_modify_callback(BTreeDescr *descr, OTuple tup, OTuple *newtup,
					   OXid oxid, OTupleXactInfo xactInfo,
					   RowLockMode *lock_mode, BTreeLocationHint *hint,
					   void *arg)
{
	OLockCallbackArg *o_arg = (OLockCallbackArg *) arg;
	TupleTableSlot *slot = o_arg->scanSlot;

	o_arg->modified = o_callback_is_modified(o_arg->oxid, o_arg->csn, xactInfo);

	if (XACT_INFO_IS_FINISHED(xactInfo))
		o_arg->csn = o_arg->modified ? (XACT_INFO_MAP_CSN(xactInfo) + 1) : o_arg->csn;
	else
		o_arg->csn = COMMITSEQNO_INPROGRESS;

	copy_tuple_to_slot(tup, slot, o_arg->descr, o_arg->csn,
					   PrimaryIndexNumber, hint);

	return OBTreeCallbackActionLock;
}

static void
get_keys_from_ps(TupleTableSlot *planSlot, OIndexDescr *id,
				 int *junkAttrs, OBTreeKeyBound *key)
{
	Datum		pkDatum;
	bool		pkNull = false;
	int			i;

	key->nkeys = id->nonLeafTupdesc->natts;

	if (id->primaryIsCtid)
	{
		key->keys[0].value = PointerGetDatum(&planSlot->tts_tid);
		key->keys[0].type = TIDOID;
		key->keys[0].flags = O_VALUE_BOUND_PLAIN_VALUE;
		key->keys[0].comparator = NULL;
	}
	else
	{
		for (i = 0; i < key->nkeys; i++)
		{
			AttrNumber	attnum;

			attnum = id->fields[i].tableAttnum;

			pkDatum = ExecGetJunkAttribute(planSlot, attnum, &pkNull);
			if (pkNull)
				elog(ERROR, "key %d is null", i);

			key->keys[i].value = pkDatum;
			key->keys[i].type = id->nonLeafTupdesc->attrs[i].atttypid;
			key->keys[i].flags = O_VALUE_BOUND_PLAIN_VALUE;
			key->keys[i].comparator = NULL;
		}
	}
}

static inline bool
is_keys_eq(BTreeDescr *desc, OBTreeKeyBound *k1, OBTreeKeyBound *k2)
{
	return (o_idx_cmp(desc,
					  (Pointer) k1, BTreeKeyBound,
					  (Pointer) k2, BTreeKeyBound) == 0);
}

static void
o_report_duplicate(Relation rel, OIndexDescr *id, TupleTableSlot *slot)
{
	bool		ctid = id->primaryIsCtid;
	bool		primary = id->desc.type == oIndexPrimary;

	if (primary && ctid)
	{
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("ctid index key duplicate.")));
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_UNIQUE_VIOLATION),
				 errmsg("duplicate key value violates unique constraint \"%s\"",
						RelationGetRelationName(rel)),
				 errdetail("Key %s already exists",
						   tss_orioledb_print_idx_key(slot, id)),
				 errtableconstraint(rel, id->desc.type == oIndexPrimary ? "pk" : "sk")));
	}
}
