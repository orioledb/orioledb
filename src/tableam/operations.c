/*-------------------------------------------------------------------------
 *
 * operations.c
 *		Implementation of table-level operations
 *
 * Copyright (c) 2021-2025, Oriole DB Inc.
 * Copyright (c) 2025, Supabase Inc.
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
#include "btree/undo.h"
#include "indexam/handler.h"
#include "recovery/recovery.h"
#include "recovery/wal.h"
#include "storage/itemptr.h"
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
#include "pgstat.h"
#include "storage/bufmgr.h"
#include "utils/datum.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"

static OTableModifyResult o_tbl_indices_overwrite(OTableDescr *descr,
												  OBTreeKeyBound *oldPkey,
												  TupleTableSlot *newSlot,
												  OXid oxid, CommitSeqNo csn,
												  BTreeLocationHint *hint,
												  OModifyCallbackArg *arg);
static OTableModifyResult o_tbl_indices_reinsert(OTableDescr *descr,
												 OBTreeKeyBound *oldPkey,
												 OBTreeKeyBound *newPkey,
												 TupleTableSlot *newSlot,
												 OXid oxid, CommitSeqNo csn,
												 BTreeLocationHint *hint,
												 OModifyCallbackArg *arg);
static OTableModifyResult o_tbl_indices_delete(OTableDescr *descr,
											   OBTreeKeyBound *key,
											   OXid oxid, CommitSeqNo csn,
											   BTreeLocationHint *hint,
											   OModifyCallbackArg *arg);
static void o_toast_insert_values(Relation rel, OTableDescr *descr,
								  TupleTableSlot *slot, OXid oxid, CommitSeqNo csn);
static inline bool o_callback_is_modified(OXid oxid, CommitSeqNo csn, OTupleXactInfo xactInfo);
static OBTreeModifyCallbackAction o_insert_callback(BTreeDescr *descr,
													OTuple tup, OTuple *newtup,
													OXid oxid, OTupleXactInfo xactInfo,
													BTreeLeafTupleDeletedStatus deleted,
													UndoLocation location,
													RowLockMode *lock_mode,
													BTreeLocationHint *hint,
													void *arg);
static OBTreeWaitCallbackAction o_insert_with_arbiter_wait_callback(BTreeDescr *descr,
																	OTuple tup, OTuple *newtup,
																	OXid oxid, OTupleXactInfo xactInfo,
																	UndoLocation location,
																	RowLockMode *lock_mode,
																	BTreeLocationHint *hint,
																	void *arg);
static OBTreeModifyCallbackAction o_insert_with_arbiter_modify_deleted_callback(BTreeDescr *descr,
																				OTuple tup, OTuple *newtup,
																				OXid oxid, OTupleXactInfo xactInfo,
																				BTreeLeafTupleDeletedStatus deleted,
																				UndoLocation location,
																				RowLockMode *lock_mode,
																				BTreeLocationHint *hint,
																				void *arg);
static OBTreeModifyCallbackAction o_insert_with_arbiter_modify_callback(BTreeDescr *descr,
																		OTuple tup, OTuple *newtup,
																		OXid oxid, OTupleXactInfo xactInfo,
																		UndoLocation location,
																		RowLockMode *lock_mode,
																		BTreeLocationHint *hint,
																		void *arg);
static OBTreeModifyCallbackAction o_delete_callback(BTreeDescr *descr,
													OTuple tup, OTuple *newtup,
													OXid oxid, OTupleXactInfo xactInfo,
													UndoLocation location,
													RowLockMode *lock_mode,
													BTreeLocationHint *hint,
													void *arg);
static OBTreeModifyCallbackAction o_delete_deleted_callback(BTreeDescr *desc,
															OTuple oldTup,
															OTuple *newTup,
															OXid oxid,
															OTupleXactInfo prevXactInfo,
															BTreeLeafTupleDeletedStatus deleted,
															UndoLocation location,
															RowLockMode *lockMode,
															BTreeLocationHint *hint,
															void *arg);
static OBTreeModifyCallbackAction o_update_callback(BTreeDescr *descr,
													OTuple tup, OTuple *newtup,
													OXid oxid, OTupleXactInfo xactInfo,
													UndoLocation location,
													RowLockMode *lock_mode,
													BTreeLocationHint *hint,
													void *arg);
static OBTreeModifyCallbackAction o_update_deleted_callback(BTreeDescr *descr,
															OTuple tup, OTuple *newtup,
															OXid oxid, OTupleXactInfo xactInfo,
															BTreeLeafTupleDeletedStatus deleted,
															UndoLocation location,
															RowLockMode *lock_mode,
															BTreeLocationHint *hint,
															void *arg);
static OBTreeWaitCallbackAction o_lock_wait_callback(BTreeDescr *descr, OTuple tup, OTuple *newtup,
													 OXid oxid, OTupleXactInfo xactInfo,
													 UndoLocation location,
													 RowLockMode *lock_mode, BTreeLocationHint *hint,
													 void *arg);
static OBTreeModifyCallbackAction o_lock_modify_callback(BTreeDescr *descr, OTuple tup, OTuple *newtup,
														 OXid oxid, OTupleXactInfo xactInfo,
														 UndoLocation location,
														 RowLockMode *lock_mode, BTreeLocationHint *hint,
														 void *arg);
static OBTreeModifyCallbackAction o_lock_deleted_callback(BTreeDescr *descr, OTuple tup, OTuple *newtup,
														  OXid oxid, OTupleXactInfo xactInfo,
														  BTreeLeafTupleDeletedStatus deleted,
														  UndoLocation location,
														  RowLockMode *lock_mode, BTreeLocationHint *hint,
														  void *arg);
static void fill_key_bound(TupleTableSlot *slot, OIndexDescr *idx, OBTreeKeyBound *bound);
static inline bool is_keys_eq(BTreeDescr *desc, OBTreeKeyBound *k1, OBTreeKeyBound *k2);
static void o_report_duplicate(Relation rel, OIndexDescr *id,
							   TupleTableSlot *slot);

PG_FUNCTION_INFO_V1(orioledb_int4range_immutable);

static TupleTableSlot *
update_arg_get_slot(OModifyCallbackArg *arg)
{
	if ((!arg->modified && (arg->options & TABLE_MODIFY_FETCH_OLD_TUPLE)) ||
		(arg->modified && (arg->options & TABLE_MODIFY_LOCK_UPDATED)))
		return arg->scanSlot;
	else
		return arg->tmpSlot;
}

void
o_apply_new_bridge_index_ctid(OTableDescr *descr, Relation relation,
							  TupleTableSlot *slot, CommitSeqNo csn)
{
	OIndexDescr *primary = GET_PRIMARY(descr);
	OTableSlot *oslot = (OTableSlot *) slot;
	bool		success;
	BTreeModifyCallbackInfo callbackInfo =
	{
		.waitCallback = NULL,
		.modifyDeletedCallback = o_insert_callback,
		.modifyCallback = NULL,
		.needsUndoForSelfCreated = true
	};
	OSnapshot	o_snapshot;
	OXid		oxid;
	TupleTableSlot *bridge_slot;
	uint32		version = 0;
	OTuple		tuple;
	Datum		values[INDEX_MAX_KEYS + 1];
	bool		isnull[INDEX_MAX_KEYS + 1];
	bool		overflow = false;

	o_btree_load_shmem(&primary->desc);
	if (descr->bridge->primaryIsCtid)
	{
		values[1] = PointerGetDatum(&slot->tts_tid);
		isnull[1] = false;
	}
	else
	{
		int			i;

		for (i = 0; i < GET_PRIMARY(descr)->nKeyFields; i++)
		{
			AttrNumber	attnum = GET_PRIMARY(descr)->tableAttnums[i] - 1;

			values[i + 1] = slot->tts_values[attnum];
			isnull[i + 1] = slot->tts_isnull[attnum];
		}
	}

	do
	{
		oslot->bridge_ctid = btree_bridge_ctid_get_and_inc(&primary->desc, &overflow);

		values[0] = PointerGetDatum(&oslot->bridge_ctid);
		isnull[0] = false;

		tuple = o_form_tuple(descr->bridge->leafTupdesc, &descr->bridge->leafSpec, version,
							 values, isnull, NULL);
		bridge_slot = descr->bridge->new_leaf_slot;
		tts_orioledb_store_tuple(bridge_slot, tuple, descr, csn, BridgeIndexNumber, false, NULL);
		callbackInfo.arg = bridge_slot;

		fill_current_oxid_osnapshot(&oxid, &o_snapshot);

		success = (o_tbl_index_insert(descr, descr->bridge, &tuple, bridge_slot,
									  oxid, o_snapshot.csn, &callbackInfo, UNIQUE_CHECK_YES) == OBTreeModifyResultInserted);

		if (!success && !overflow)
			o_report_duplicate(relation, descr->bridge, bridge_slot);
	} while (!success);

	if (primary->desc.storageType == BTreeStoragePersistence)
	{
		o_wal_insert(&descr->bridge->desc, tuple, REPLICA_IDENTITY_DEFAULT);
		flush_local_wal(false, false);
	}

	if (tuple.data)
		pfree(tuple.data);
}

static void
delete_old_bridge_index_ctid(OTableDescr *descr, Relation relation,
							 ItemPointer iptr, CommitSeqNo csn)
{
	OIndexDescr *primary = GET_PRIMARY(descr);
	OSnapshot	o_snapshot;
	OXid		oxid;
	TupleTableSlot *bridge_slot;
	OTableSlot *bridge_oslot;
	OTableModifyResult result PG_USED_FOR_ASSERTS_ONLY;

	bridge_slot = descr->bridge->new_leaf_slot;
	bridge_oslot = (OTableSlot *) bridge_slot;
	ItemPointerCopy(iptr, &bridge_oslot->bridge_ctid);

	fill_current_oxid_osnapshot(&oxid, &o_snapshot);

	result = o_tbl_index_delete(descr->bridge, BridgeIndexNumber, bridge_slot,
								oxid, o_snapshot.csn);

	if (primary->desc.storageType == BTreeStoragePersistence)
	{
		OTuple		keyTuple;

		keyTuple.formatFlags = O_TUPLE_FLAGS_FIXED_FORMAT;
		keyTuple.data = (Pointer) &bridge_oslot->bridge_ctid;

		/*
		 * o_wal_delete_key can be used as long as bridge index can't have
		 * replica identity
		 */
		o_wal_delete_key(&descr->bridge->desc, keyTuple, true);
		flush_local_wal(false, false);
	}

	if (!result.success)
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("Couldn't delete old bridge ctid: %s",
							   tss_orioledb_print_idx_key(bridge_slot, descr->bridge))));
}

/*  TODO: Merge with o_apply_new_bridge_index_ctid */
static void
reinsert_bridge_ctid_on_pkey_changed(OTableDescr *descr, Relation relation,
									 TupleTableSlot *slot, CommitSeqNo csn)
{
	OIndexDescr *primary = GET_PRIMARY(descr);
	OTableSlot *oslot = (OTableSlot *) slot;
	bool		success;
	BTreeModifyCallbackInfo callbackInfo =
	{
		.waitCallback = NULL,
		.modifyDeletedCallback = o_insert_callback,
		.modifyCallback = NULL,
		.needsUndoForSelfCreated = true
	};
	OSnapshot	o_snapshot;
	OXid		oxid;
	TupleTableSlot *bridge_slot;
	uint32		version = 0;
	OTuple		tuple;
	Datum		values[INDEX_MAX_KEYS + 1];
	bool		isnull[INDEX_MAX_KEYS + 1];
	bool		overflow = false;

	slot_getallattrs(slot);

	o_btree_load_shmem(&primary->desc);
	if (descr->bridge->primaryIsCtid)
	{
		values[1] = PointerGetDatum(&slot->tts_tid);
		isnull[1] = false;
	}
	else
	{
		int			i;

		for (i = 0; i < GET_PRIMARY(descr)->nKeyFields; i++)
		{
			AttrNumber	attnum = GET_PRIMARY(descr)->tableAttnums[i] - 1;

			values[i + 1] = slot->tts_values[attnum];
			isnull[i + 1] = slot->tts_isnull[attnum];
		}
	}

	do
	{
		values[0] = PointerGetDatum(&oslot->bridge_ctid);
		isnull[0] = false;

		tuple = o_form_tuple(descr->bridge->leafTupdesc, &descr->bridge->leafSpec, version,
							 values, isnull, NULL);
		bridge_slot = descr->bridge->new_leaf_slot;
		tts_orioledb_store_tuple(bridge_slot, tuple, descr, csn, BridgeIndexNumber, false, NULL);
		callbackInfo.arg = bridge_slot;

		fill_current_oxid_osnapshot(&oxid, &o_snapshot);

		success = (o_tbl_index_insert(descr, descr->bridge, &tuple, bridge_slot,
									  oxid, o_snapshot.csn, &callbackInfo, UNIQUE_CHECK_YES) == OBTreeModifyResultInserted);

		if (!success && !overflow)
			o_report_duplicate(relation, descr->bridge, bridge_slot);
	} while (!success);

	if (primary->desc.storageType == BTreeStoragePersistence)
	{
		o_wal_insert(&descr->bridge->desc, tuple, REPLICA_IDENTITY_DEFAULT);
		flush_local_wal(false, false);
	}

	if (tuple.data)
		pfree(tuple.data);
}

TupleTableSlot *
o_tbl_insert(OTableDescr *descr, Relation relation,
			 TupleTableSlot *slot, OXid oxid, CommitSeqNo csn)
{
	OTableModifyResult mres;
	OTuple		tup;
	OIndexDescr *primary = GET_PRIMARY(descr);
	BTreeModifyCallbackInfo callbackInfo =
	{
		.waitCallback = NULL,
		.modifyDeletedCallback = o_insert_callback,
		.modifyCallback = NULL,
		.needsUndoForSelfCreated = false,
		.arg = slot
	};

	CheckCmdReplicaIdentity(relation, CMD_INSERT);

	if (slot->tts_ops != descr->newTuple->tts_ops ||
		(((OTableSlot *) slot)->descr != NULL &&
		 ((OTableSlot *) slot)->descr != descr))
	{
		((OTableSlot *) descr->newTuple)->descr = descr;
		ExecCopySlot(descr->newTuple, slot);
		slot = descr->newTuple;
	}

	if (GET_PRIMARY(descr)->primaryIsCtid)
	{
		ItemPointerData iptr;

		o_btree_load_shmem(&primary->desc);
		iptr = btree_ctid_get_and_inc(&primary->desc);
		tts_orioledb_set_ctid(slot, &iptr);
	}

	if (descr->bridge)
		o_apply_new_bridge_index_ctid(descr, relation, slot, csn);

	tts_orioledb_toast(slot, descr);

	tup = tts_orioledb_form_tuple(slot, descr);
	o_btree_check_size_of_tuple(o_tuple_size(tup, &primary->leafSpec),
								RelationGetRelationName(relation),
								false);

	mres.success = (o_tbl_index_insert(descr, descr->indices[0], NULL, slot,
									   oxid, csn, &callbackInfo, UNIQUE_CHECK_YES) == OBTreeModifyResultInserted);
	if (!mres.success)
	{
		mres.failedIxNum = 0;
		mres.action = BTreeOperationInsert;
		mres.oldTuple = NULL;

		o_report_duplicate(relation, descr->indices[mres.failedIxNum], slot);
	}
	else
		pgstat_count_heap_insert(relation, 1);

	o_toast_insert_values(relation, descr, slot, oxid, csn);

	/* Tuple might be changed in the callback */
	tup = tts_orioledb_form_tuple(slot, descr);
	if (primary->desc.storageType == BTreeStoragePersistence)
		o_wal_insert(&primary->desc, tup, relation->rd_rel->relreplident);

	return slot;
}

static RowLockMode
tuple_lock_mode_to_row_lock_mode(LockTupleMode mode)
{
	switch (mode)
	{
		case LockTupleKeyShare:
			return RowLockKeyShare;
		case LockTupleShare:
			return RowLockShare;
		case LockTupleNoKeyExclusive:
			return RowLockNoKeyUpdate;
		case LockTupleExclusive:
			return RowLockUpdate;
		default:
			elog(ERROR, "Unknown lock mode: %u", mode);
			break;
	}
	return RowLockUpdate;		/* keep compiler quiet */
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
		.modifyDeletedCallback = o_lock_deleted_callback,
		.modifyCallback = o_lock_modify_callback,
		.needsUndoForSelfCreated = true,
		.arg = larg
	};

	o_btree_load_shmem(&GET_PRIMARY(descr)->desc);

	lock_mode = tuple_lock_mode_to_row_lock_mode(mode);

	O_TUPLE_SET_NULL(nullTup);
	res = o_btree_modify(&GET_PRIMARY(descr)->desc, BTreeOperationLock,
						 nullTup, BTreeKeyNone, (Pointer) pkey, BTreeKeyBound,
						 oxid, larg->csn, lock_mode,
						 hint, &callbackInfo);

	Assert(res == OBTreeModifyResultLocked || res == OBTreeModifyResultFound || res == OBTreeModifyResultNotFound);

	return res;
}

static void
fill_pkey_bound(TupleTableSlot *slot, OIndexDescr *idx, OBTreeKeyBound *pkey)
{
	OTableSlot *oslot = (OTableSlot *) slot;

	slot_getsomeattrs(slot, idx->leafTupdesc->natts);

	if (idx->primaryIsCtid)
	{
		Datum		value;

		pkey->nkeys = 1;
		if (idx->bridging)
			value = PointerGetDatum(&oslot->bridge_ctid);
		else
			value = PointerGetDatum(&slot->tts_tid);

		pkey->keys[0].value = value;
		pkey->keys[0].type = TIDOID;
		pkey->keys[0].flags = O_VALUE_BOUND_PLAIN_VALUE;
		pkey->keys[0].comparator = idx->fields[0].comparator;
		pkey->keys[0].exclusion_fn = NULL;
	}
	else
	{
		int			i;
		int			pk_from;

		pk_from = idx->nFields - idx->nPrimaryFields;

		pkey->nkeys = idx->nPrimaryFields;
		for (i = 0; i < idx->nPrimaryFields; i++)
		{
			AttrNumber	attnum = idx->primaryFieldsAttnums[i];

			pkey->keys[i].value = slot->tts_values[attnum - 1];
			pkey->keys[i].type = idx->leafTupdesc->attrs[pk_from + i].atttypid;
			pkey->keys[i].flags = O_VALUE_BOUND_PLAIN_VALUE;
			if (slot->tts_isnull[attnum - 1])
				pkey->keys[i].flags |= O_VALUE_BOUND_NULL;
			pkey->keys[i].comparator = idx->fields[pk_from + i].comparator;
			pkey->keys[i].exclusion_fn = NULL;
		}
	}
}

static void
bridged_index_fill_pkey_bound(TupleTableSlot *slot, OIndexDescr *primary, OBTreeKeyBound *pkey)
{
	if (primary->primaryIsCtid)
	{
		Datum		value;

		pkey->nkeys = 1;
		value = PointerGetDatum(&slot->tts_tid);

		pkey->keys[0].value = value;
		pkey->keys[0].type = TIDOID;
		pkey->keys[0].flags = O_VALUE_BOUND_PLAIN_VALUE;
		pkey->keys[0].comparator = primary->fields[0].comparator;
		pkey->keys[0].exclusion_fn = NULL;
	}
	else
	{
		int			i;

		pkey->nkeys = primary->nKeyFields;
		for (i = 0; i < primary->nKeyFields; i++)
		{
			int			attnum = primary->tableAttnums[i];

			pkey->keys[i].value = slot->tts_values[attnum - 1];
			pkey->keys[i].type = primary->leafTupdesc->attrs[attnum - 1].atttypid;
			pkey->keys[i].flags = O_VALUE_BOUND_PLAIN_VALUE;
			if (slot->tts_isnull[attnum - 1])
				pkey->keys[i].flags |= O_VALUE_BOUND_NULL;
			pkey->keys[i].comparator = primary->fields[attnum - 1].comparator;
			pkey->keys[i].exclusion_fn = NULL;
		}
	}
}

static bool
OExecCheckIndexConstraints(ResultRelInfo *resultRelInfo, TupleTableSlot *slot,
						   EState *estate, Datum *conflictTid,
						   List *arbiterIndexes)
{
	int			i;
	int			numIndices;
	RelationPtr relationDescs;
	Relation	heapRelation;
	IndexInfo **indexInfoArray;
	ExprContext *econtext;
	Datum		values[INDEX_MAX_KEYS];
	bool		isnull[INDEX_MAX_KEYS];
	ItemPointerData invalidItemPtr;
	Datum		invalidItemPtrDatum;
	bool		checkedIndex = false;

	ItemPointerSetInvalid(&invalidItemPtr);
	invalidItemPtrDatum = ItemPointerGetDatum(&invalidItemPtr);

	/*
	 * Get information from the result relation info structure.
	 */
	numIndices = resultRelInfo->ri_NumIndices;
	relationDescs = resultRelInfo->ri_IndexRelationDescs;
	indexInfoArray = resultRelInfo->ri_IndexRelationInfo;
	heapRelation = resultRelInfo->ri_RelationDesc;

	/*
	 * We will use the EState's per-tuple context for evaluating predicates
	 * and index expressions (creating it if it's not already there).
	 */
	econtext = GetPerTupleExprContext(estate);

	/* Arrange for econtext's scan tuple to be the tuple under test */
	econtext->ecxt_scantuple = slot;

	/*
	 * For each index, form index tuple and check if it satisfies the
	 * constraint.
	 */
	Assert(numIndices > 0);		/* for clang-analyzer */
	for (i = 0; i < numIndices; i++)
	{
		Relation	indexRelation = relationDescs[i];
		IndexInfo  *indexInfo;
		bool		satisfiesConstraint;
		OBTOptions *options;

		if (indexRelation == NULL)
			continue;

		options = (OBTOptions *) indexRelation->rd_options;

		if (indexRelation->rd_rel->relam == BTREE_AM_OID && !(options && !options->orioledb_index))
			continue;

		indexInfo = indexInfoArray[i];

		if (!indexInfo->ii_Unique && !indexInfo->ii_ExclusionOps)
			continue;

		/* If the index is marked as read-only, ignore it */
		if (!indexInfo->ii_ReadyForInserts)
			continue;

		/* When specific arbiter indexes requested, only examine them */
		if (arbiterIndexes != NIL &&
			!list_member_oid(arbiterIndexes,
							 indexRelation->rd_index->indexrelid))
			continue;

		if (!indexRelation->rd_index->indimmediate)
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("ON CONFLICT does not support deferrable unique constraints/exclusion constraints as arbiters"),
					 errtableconstraint(heapRelation,
										RelationGetRelationName(indexRelation))));

		checkedIndex = true;

		/* Check for partial index */
		if (indexInfo->ii_Predicate != NIL)
		{
			ExprState  *predicate;

			/*
			 * If predicate state not set up yet, create it (in the estate's
			 * per-query context)
			 */
			predicate = indexInfo->ii_PredicateState;
			if (predicate == NULL)
			{
				predicate = ExecPrepareQual(indexInfo->ii_Predicate, estate);
				indexInfo->ii_PredicateState = predicate;
			}

			/* Skip this index-update if the predicate isn't satisfied */
			if (!ExecQual(predicate, econtext))
				continue;
		}

		/*
		 * FormIndexDatum fills in its values and isnull parameters with the
		 * appropriate values for the column(s) of the index.
		 */
		FormIndexDatum(indexInfo,
					   slot,
					   estate,
					   values,
					   isnull);

		satisfiesConstraint =
			check_exclusion_or_unique_constraint(heapRelation, indexRelation,
												 indexInfo, invalidItemPtrDatum,
												 values, isnull, estate, false,
												 CEOUC_WAIT, true,
												 conflictTid);
		if (!satisfiesConstraint)
			return false;
	}

	if (arbiterIndexes != NIL && !checkedIndex)
		elog(ERROR, "unexpected failure to find arbiter index");

	return true;
}

static List *
OExecInsertIndexTuples(ResultRelInfo *resultRelInfo,
					   TupleTableSlot *slot,
					   EState *estate,
					   bool *specConflict,
					   List *arbiterIndexes)
{
	List	   *result = NIL;
	int			i;
	int			numIndices;
	RelationPtr relationDescs;
	Relation	heapRelation;
	IndexInfo **indexInfoArray;
	ExprContext *econtext;
	Datum		values[INDEX_MAX_KEYS];
	bool		isnull[INDEX_MAX_KEYS];
	Datum		tupleidDatum;

	/*
	 * Get information from the result relation info structure.
	 */
	numIndices = resultRelInfo->ri_NumIndices;
	relationDescs = resultRelInfo->ri_IndexRelationDescs;
	indexInfoArray = resultRelInfo->ri_IndexRelationInfo;
	heapRelation = resultRelInfo->ri_RelationDesc;

	/* Sanity check: slot must belong to the same rel as the resultRelInfo. */
	Assert(slot->tts_tableOid == RelationGetRelid(heapRelation));

	if (table_get_row_ref_type(heapRelation) == ROW_REF_ROWID)
	{
		bool		isnull;

		tupleidDatum = slot_getsysattr(slot, RowIdAttributeNumber, &isnull);
		Assert(!isnull);
	}
	else
	{
		Assert(ItemPointerIsValid(&slot->tts_tid));
		tupleidDatum = ItemPointerGetDatum(&slot->tts_tid);
	}

	/*
	 * We will use the EState's per-tuple context for evaluating predicates
	 * and index expressions (creating it if it's not already there).
	 */
	econtext = GetPerTupleExprContext(estate);

	/* Arrange for econtext's scan tuple to be the tuple under test */
	econtext->ecxt_scantuple = slot;

	/*
	 * for each index, form and insert the index tuple
	 */
	for (i = 0; i < numIndices; i++)
	{
		Relation	indexRelation = relationDescs[i];
		IndexInfo  *indexInfo;
		bool		applyNoDupErr;
		IndexUniqueCheck checkUnique;
		bool		satisfiesConstraint;
		OBTOptions *options;

		if (indexRelation == NULL)
			continue;

		options = (OBTOptions *) indexRelation->rd_options;

		if (indexRelation->rd_rel->relam == BTREE_AM_OID && !(options && !options->orioledb_index))
			continue;

		indexInfo = indexInfoArray[i];

		/* If the index is marked as read-only, ignore it */
		if (!indexInfo->ii_ReadyForInserts)
			continue;

		/* Check for partial index */
		if (indexInfo->ii_Predicate != NIL)
		{
			ExprState  *predicate;

			/*
			 * If predicate state not set up yet, create it (in the estate's
			 * per-query context)
			 */
			predicate = indexInfo->ii_PredicateState;
			if (predicate == NULL)
			{
				predicate = ExecPrepareQual(indexInfo->ii_Predicate, estate);
				indexInfo->ii_PredicateState = predicate;
			}

			/* Skip this index-update if the predicate isn't satisfied */
			if (!ExecQual(predicate, econtext))
				continue;
		}

		/*
		 * FormIndexDatum fills in its values and isnull parameters with the
		 * appropriate values for the column(s) of the index.
		 */
		FormIndexDatum(indexInfo,
					   slot,
					   estate,
					   values,
					   isnull);

		/* Check whether to apply noDupErr to this index */
		applyNoDupErr =
			(arbiterIndexes == NIL ||
			 list_member_oid(arbiterIndexes,
							 indexRelation->rd_index->indexrelid));

		/*
		 * The index AM does the actual insertion, plus uniqueness checking.
		 *
		 * For an immediate-mode unique index, we just tell the index AM to
		 * throw error if not unique.
		 *
		 * For a deferrable unique index, we tell the index AM to just detect
		 * possible non-uniqueness, and we add the index OID to the result
		 * list if further checking is needed.
		 *
		 * For a speculative insertion (used by INSERT ... ON CONFLICT), do
		 * the same as for a deferrable unique index.
		 */
		if (!indexRelation->rd_index->indisunique)
			checkUnique = UNIQUE_CHECK_NO;
		else if (applyNoDupErr)
			checkUnique = UNIQUE_CHECK_PARTIAL;
		else if (indexRelation->rd_index->indimmediate)
			checkUnique = UNIQUE_CHECK_YES;
		else
			checkUnique = UNIQUE_CHECK_PARTIAL;

		satisfiesConstraint =
			index_insert(indexRelation, /* index relation */
						 values,	/* array of index Datums */
						 isnull,	/* null flags */
						 tupleidDatum,	/* tid of heap tuple */
						 heapRelation,	/* heap relation */
						 checkUnique,	/* type of uniqueness check to do */
						 false, /* UPDATE without logical change? */
						 indexInfo);	/* index AM may need this */

		/*
		 * If the index has an associated exclusion constraint, check that.
		 * This is simpler than the process for uniqueness checks since we
		 * always insert first and then check.  If the constraint is deferred,
		 * we check now anyway, but don't throw error on violation or wait for
		 * a conclusive outcome from a concurrent insertion; instead we'll
		 * queue a recheck event.  Similarly, noDupErr callers (speculative
		 * inserters) will recheck later, and wait for a conclusive outcome
		 * then.
		 *
		 * An index for an exclusion constraint can't also be UNIQUE (not an
		 * essential property, we just don't allow it in the grammar), so no
		 * need to preserve the prior state of satisfiesConstraint.
		 */
		if (indexInfo->ii_ExclusionOps != NULL)
		{
			bool		violationOK;
			CEOUC_WAIT_MODE waitMode;

			if (applyNoDupErr)
			{
				violationOK = true;
				waitMode = CEOUC_LIVELOCK_PREVENTING_WAIT;
			}
			else if (!indexRelation->rd_index->indimmediate)
			{
				violationOK = true;
				waitMode = CEOUC_NOWAIT;
			}
			else
			{
				violationOK = false;
				waitMode = CEOUC_WAIT;
			}

			satisfiesConstraint =
				check_exclusion_or_unique_constraint(heapRelation,
													 indexRelation, indexInfo,
													 tupleidDatum, values, isnull,
													 estate, false,
													 waitMode, violationOK, NULL);
		}

		if ((checkUnique == UNIQUE_CHECK_PARTIAL ||
			 indexInfo->ii_ExclusionOps != NULL) &&
			!satisfiesConstraint)
		{
			/*
			 * The tuple potentially violates the uniqueness or exclusion
			 * constraint, so make a note of the index so that we can re-check
			 * it later.  Speculative inserters are told if there was a
			 * speculative conflict, since that always requires a restart.
			 */
			result = lappend_oid(result, RelationGetRelid(indexRelation));
			if (indexRelation->rd_index->indimmediate && specConflict)
				*specConflict = true;
		}
	}

	return result;
}

int
o_exclusion_cmp(OIndexDescr *id, OBTreeKeyBound *key1, OTuple *tuple2)
{
	TupleDesc	tupdesc;
	OTupleFixedFormatSpec *spec;
	int			i,
				attnum;
	Datum		value;
	bool		isnull;

	tupdesc = id->leafTupdesc;
	spec = &id->leafSpec;

	Assert(id->nKeyFields > 0); /* for clang-analyzer */
	for (i = 0; i < id->nKeyFields; i++)
	{
		uint8		flags = key1->keys[i].flags;
		int			cmp;

		if (flags & O_VALUE_BOUND_UNBOUNDED)
			return (flags & O_VALUE_BOUND_LOWER) ? -1 : 1;

		attnum = i + 1;
		value = o_fastgetattr(*tuple2, attnum, tupdesc, spec, &isnull);

		cmp = o_idx_cmp_range_key_to_value(&key1->keys[i], &id->fields[i],
										   value, isnull);
		if (cmp != 0)
			return cmp;
	}

	return 0;
}

static void
exclusion_fill_bound(TupleTableSlot *slot, OIndexDescr *idx, OBTreeKeyBound *bound)
{
	int			i;
	int			ctid_off = idx->primaryIsCtid ? 1 : 0;
	ListCell   *indexpr_item = list_head(idx->expressions_state);

	slot_getsomeattrs(slot, idx->maxTableAttnum - ctid_off);

	bound->nkeys = idx->nonLeafTupdesc->natts;
	Assert(bound->nkeys > 0);	/* for clang-analyzer */
	for (i = 0; i < bound->nkeys; i++)
	{
		Datum		value;
		bool		isnull;
		int			attnum;
		Oid			typid;

		attnum = idx->tableAttnums[i];

		if (attnum != EXPR_ATTNUM)
			value = o_get_tbl_att(slot, attnum, idx->primaryIsCtid,
								  &isnull, &typid);
		else
		{
			value = o_get_idx_expr_att(slot, idx,
									   (ExprState *) lfirst(indexpr_item),
									   &isnull);
			typid = idx->nonLeafTupdesc->attrs[i].atttypid;
			indexpr_item = lnext(idx->expressions_state, indexpr_item);
		}

		bound->keys[i].value = value;
		bound->keys[i].type = typid;
		bound->keys[i].flags = O_VALUE_BOUND_PLAIN_VALUE;
		if (isnull)
			bound->keys[i].flags |= O_VALUE_BOUND_NULL;
		bound->keys[i].comparator = idx->fields[i].comparator;
		if (idx->fields[i].exclusion_fn)
			bound->keys[i].exclusion_fn = idx->fields[i].exclusion_fn;
		else
			bound->keys[i].exclusion_fn = NULL;
	}
}

static bool
o_check_exclusion_constraint(OTableDescr *descr, OIndexDescr *index, TupleTableSlot *slot)
{
	OSnapshot	o_snapshot;
	OXid		oxid;
	BTreeIterator *iter;
	OTuple		tuple;
	OBTreeKeyBound bound;

	fill_current_oxid_osnapshot(&oxid, &o_snapshot);
	iter = o_btree_iterator_create(&index->desc, NULL, BTreeKeyNone, &o_snapshot, ForwardScanDirection);
	tuple = o_btree_iterator_fetch(iter, NULL, NULL, BTreeKeyNone, true, NULL);
	slot_getallattrs(slot);
	exclusion_fill_bound(slot, index, &bound);
	while (!O_TUPLE_IS_NULL(tuple))
	{
		int			res = o_exclusion_cmp(index, &bound, &tuple);

		if (res == 0)
		{
			res = o_idx_cmp(&index->desc,
							(Pointer) &bound, BTreeKeyBound,
							(Pointer) &tuple, BTreeKeyLeafTuple);
			if (res != 0)
				return false;
		}

		tuple = o_btree_iterator_fetch(iter, NULL, NULL,
									   BTreeKeyNone, true, NULL);

	}
	btree_iterator_free(iter);

	return true;
}

TupleTableSlot *
o_tbl_insert_with_arbiter(Relation rel,
						  OTableDescr *descr,
						  TupleTableSlot *slot,
						  List *arbiterIndexes,
						  CommandId cid,
						  LockTupleMode lockmode,
						  TupleTableSlot *lockedSlot,
						  EState *estate,
						  ResultRelInfo *resultRelInfo)
{
	InsertOnConflictCallbackArg ioc_arg;
	UndoStackLocations undoStackLocations;
	OTuple		tup;
	OSnapshot	oSnapshot = {0};
	CommitSeqNo csn;
	OXid		oxid;
	Datum		conflictTid;

	fill_current_oxid_osnapshot(&oxid, &oSnapshot);
	csn = oSnapshot.csn;
	get_cur_undo_locations(&undoStackLocations, UndoLogRegular);

	ioc_arg.desc = descr;
	ioc_arg.oxid = oxid;
	ioc_arg.newSlot = (OTableSlot *) slot;
	ioc_arg.lockMode = tuple_lock_mode_to_row_lock_mode(lockmode);
	ioc_arg.scanSlot = lockedSlot;
	ioc_arg.tupUndoLocation = InvalidUndoLocation;

	while (true)
	{
		CommitSeqNo save_csn = csn;
		int			i,
					failedIndexNumber = -1;
		bool		success = true;
		bool		specConflict = false;

		BTreeModifyCallbackInfo callbackInfo = {
			.waitCallback = o_insert_with_arbiter_wait_callback,
			.modifyDeletedCallback = o_insert_with_arbiter_modify_deleted_callback,
			.modifyCallback = o_insert_with_arbiter_modify_callback,
			.needsUndoForSelfCreated = true,
			.arg = &ioc_arg
		};

		if (lockedSlot)
			ExecClearTuple(lockedSlot);
		ioc_arg.copyPrimaryOxid = false;
		ioc_arg.conflictOxid = InvalidOXid;
		ioc_arg.csn = csn;

		for (i = 0; (i < descr->nIndices) && success; i++)
		{
			OBTreeModifyResult result;

			if (arbiterIndexes != NIL &&
				!list_member_oid(arbiterIndexes, descr->indices[i]->oids.reloid))
				continue;

			if ((descr->indices[i]->desc.type == oIndexExclusion ||
				 descr->indices[i]->desc.type == oIndexUnique) &&
				!descr->indices[i]->immediate)
				ereport(ERROR,
						(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						 errmsg("ON CONFLICT does not support deferrable unique constraints/exclusion constraints as arbiters"),
						 errtableconstraint(resultRelInfo->ri_RelationDesc,
											descr->indices[i]->name.data)));

			ioc_arg.conflictIxNum = i;
			result = o_tbl_index_insert(descr, descr->indices[i], NULL, slot,
										oxid, csn, &callbackInfo,
										descr->indices[i]->desc.type == oIndexExclusion ? UNIQUE_CHECK_NO : UNIQUE_CHECK_YES);
			if (result != OBTreeModifyResultInserted)
			{
				success = false;
				failedIndexNumber = i;
			}
			else if (descr->indices[i]->desc.type == oIndexExclusion)
			{
				if (!o_check_exclusion_constraint(descr, descr->indices[i], slot))
				{
					success = false;
					failedIndexNumber = i;
				}

			}
		}

		if (descr->bridge)
		{
			OExecCheckIndexConstraints(resultRelInfo, slot, estate, &conflictTid,
									   arbiterIndexes);
			OExecInsertIndexTuples(resultRelInfo, slot,
								   estate, &specConflict,
								   arbiterIndexes);

			if (specConflict)
			{
				if (lockedSlot)
				{
					bytea	   *rowid;
					Pointer		p;
					OIndexDescr *primary = GET_PRIMARY(descr);

					ExecCopySlot(lockedSlot, slot);

					rowid = DatumGetByteaP(conflictTid);
					p = (Pointer) rowid + MAXALIGN(VARHDRSZ);

					if (!GET_PRIMARY(descr)->primaryIsCtid)
					{
						ORowIdAddendumNonCtid *add;
						OTuple		tuple;

						add = (ORowIdAddendumNonCtid *) p;
						p += MAXALIGN(sizeof(ORowIdAddendumNonCtid));

						tuple.data = p;
						tuple.formatFlags = add->flags;

						for (i = 0; i < primary->nKeyFields; i++)
						{
							int			attnum;

							attnum = primary->tableAttnums[i];

							lockedSlot->tts_values[attnum - 1] = o_fastgetattr(tuple, attnum,
																			   primary->leafTupdesc,
																			   &primary->leafSpec,
																			   &lockedSlot->tts_isnull[i]);
						}
					}
					else
					{
						p += MAXALIGN(sizeof(ORowIdAddendumCtid));
						lockedSlot->tts_tid = *(ItemPointer) p;
					}
				}

				success = false;
			}
		}

		ioc_arg.copyPrimaryOxid = true;
		for (i = 0; (i < descr->nIndices) && success; i++)
		{
			OBTreeModifyResult result;

			if (arbiterIndexes == NIL ||
				list_member_oid(arbiterIndexes, descr->indices[i]->oids.reloid))
				continue;

			ioc_arg.conflictIxNum = InvalidIndexNumber;
			result = o_tbl_index_insert(descr, descr->indices[i], NULL, slot,
										oxid, csn, &callbackInfo, UNIQUE_CHECK_YES);

			if (result != OBTreeModifyResultInserted)
			{
				success = false;
				failedIndexNumber = i;
			}
		}

		/* Successful insert case */
		if (success)
		{
			BTreeDescr *primary = &GET_PRIMARY(descr)->desc;

			pgstat_count_heap_insert(rel, 1);

			/* all inserts are OK */
			tts_orioledb_insert_toast_values(slot, descr, oxid, csn);

			tup = tts_orioledb_form_tuple(slot, descr);
			if (primary->storageType == BTreeStoragePersistence)
				o_wal_insert(primary, tup, rel->rd_rel->relreplident);
			return slot;
		}

		/* Conflict on non-arbiter index case */
		if (!success && !specConflict && !OXidIsValid(ioc_arg.conflictOxid) &&
			arbiterIndexes != NIL &&
			!list_member_oid(arbiterIndexes, descr->indices[failedIndexNumber]->oids.reloid))
		{
			o_report_duplicate(rel, descr->indices[failedIndexNumber], slot);
		}

		/* Successful lock case */
		if (!specConflict && ioc_arg.conflictIxNum == PrimaryIndexNumber)
		{
			Assert(failedIndexNumber == PrimaryIndexNumber);
			if (lockedSlot)
			{
				Assert(ioc_arg.scanSlot == lockedSlot);
				Assert(!TTS_EMPTY(lockedSlot));

				if (COMMITSEQNO_IS_INPROGRESS(ioc_arg.csn) &&
					(ioc_arg.oxid == get_current_oxid_if_any()) &&
					UndoLocationIsValid(ioc_arg.tupUndoLocation) &&
					(undo_location_get_command(ioc_arg.tupUndoLocation) >= cid))
				{
					ereport(ERROR,
							(errcode(ERRCODE_CARDINALITY_VIOLATION),
					/* translator: %s is a SQL command name */
							 errmsg("%s command cannot affect row a second time",
									"ON CONFLICT DO UPDATE"),
							 errhint("Ensure that no rows proposed for insertion within the same command have duplicate constrained values.")));
				}
				STOPEVENT(STOPEVENT_IOC_BEFORE_UPDATE, NULL);
			}
			return NULL;
		}

		/* Failed to insert.  Rollback the changes we managed to make. */
		release_undo_size(UndoLogRegular);
		apply_undo_stack(UndoLogRegular, oxid, &undoStackLocations, true);
		oxid_notify_all();

		/* Conflish with running oxid case */
		if (OXidIsValid(ioc_arg.conflictOxid))
		{
			/* helps avoid deadlocks */
			(void) wait_for_oxid(ioc_arg.conflictOxid, false);
			continue;
		}

		csn = ioc_arg.csn;

		if (lockedSlot)
		{
			OIndexDescr *primary_td = GET_PRIMARY(descr),
					   *conflict_td = descr->indices[failedIndexNumber];
			OBTreeKeyBound key,
						key2;
			BTreeLocationHint hint = {OInvalidInMemoryBlkno, 0};
			OLockCallbackArg larg;
			OBTreeModifyResult lockResult;
			TupleDesc	saved_td;

			Assert(failedIndexNumber >= 0 || specConflict);
			Assert(!TTS_EMPTY(lockedSlot));

			STOPEVENT(STOPEVENT_IOC_BEFORE_UPDATE, NULL);

			if (!specConflict)
			{
				/*
				 * HACK: we save index tuple to slot during
				 * o_insert_with_arbiter_modify_callback, but lockedSlot is
				 * for table tuple here
				 */
				saved_td = lockedSlot->tts_tupleDescriptor;
				lockedSlot->tts_tupleDescriptor = conflict_td->leafTupdesc;
				fill_pkey_bound(lockedSlot, conflict_td, &key);
				lockedSlot->tts_tupleDescriptor = saved_td;
			}
			else
				bridged_index_fill_pkey_bound(lockedSlot, primary_td, &key);
			o_btree_load_shmem(&primary_td->desc);

			larg.rel = rel;
			larg.descr = descr;
			larg.oxid = oxid;
			larg.csn = csn;
			larg.scanSlot = lockedSlot;
			larg.waitPolicy = LockWaitBlock;
			larg.wouldBlock = false;
			larg.modified = false;
			larg.selfModified = false;
			larg.deleted = BTreeLeafTupleNonDeleted;
			larg.tupUndoLocation = InvalidUndoLocation;
			larg.modifyCid = cid;

			lockResult = o_tbl_lock(descr, &key, lockmode, oxid, &larg, &hint);

			if (larg.selfModified)
			{
				ereport(ERROR,
						(errcode(ERRCODE_CARDINALITY_VIOLATION),
				/* translator: %s is a SQL command name */
						 errmsg("%s command cannot affect row a second time",
								"ON CONFLICT DO UPDATE"),
						 errhint("Ensure that no rows proposed for insertion within the same command have duplicate constrained values.")));
			}

			if (lockResult == OBTreeModifyResultNotFound)
			{
				/* concurrent modify happens */
				csn = save_csn;
				continue;
			}

			if (!specConflict)
			{
				Assert(!TTS_EMPTY(lockedSlot));

				tts_orioledb_fill_key_bound(slot,
											conflict_td,
											&key);
				tts_orioledb_fill_key_bound(lockedSlot,
											conflict_td,
											&key2);

				if (o_idx_cmp(&conflict_td->desc,
							  (Pointer) &key, BTreeKeyUniqueLowerBound,
							  (Pointer) &key2, BTreeKeyUniqueLowerBound) != 0)
				{
					/* secondary key on primary tuple has been updated */
					release_undo_size(UndoLogRegular);
					apply_undo_stack(UndoLogRegular, oxid, &undoStackLocations, true);
					oxid_notify_all();
					csn = save_csn;
					continue;
				}
			}
		}
		return NULL;
	}

	Assert(false);
	return NULL;
}

OTableModifyResult
o_tbl_update(OTableDescr *descr, TupleTableSlot *slot,
			 OBTreeKeyBound *oldPkey, Relation rel, OXid oxid,
			 CommitSeqNo csn, BTreeLocationHint *hint,
			 OModifyCallbackArg *arg, ItemPointer bridge_ctid)
{
	TupleTableSlot *oldSlot;
	OTableModifyResult mres;
	OBTreeKeyBound newPkey;
	OTuple		newTup;
	OIndexDescr *primary = GET_PRIMARY(descr);
	bool		touched_indices = false;

	CheckCmdReplicaIdentity(rel, CMD_UPDATE);

	if (slot->tts_ops != descr->newTuple->tts_ops)
	{
		ExecCopySlot(descr->newTuple, slot);
		slot = descr->newTuple;
	}

	if (primary->primaryIsCtid)
	{
		Assert(oldPkey->nkeys == 1);
		Assert(DatumGetPointer(oldPkey->keys[0].value));
		slot->tts_tid = *((ItemPointerData *) DatumGetPointer(oldPkey->keys[0].value));
	}

	if (bridge_ctid)
	{
		OTableSlot *oslot = (OTableSlot *) slot;

		oslot->bridge_ctid = *bridge_ctid;
	}

	if (descr->bridge)
	{
		List	   *indexIds;
		ListCell   *indexId;
		int			attnum;
		TupleTableSlot *newSlot;
		Bitmapset  *changed_attrs = NULL;

		/* not using simple reindex_relation here anymore, */
		/* because we hold a lock on relation already */
		indexIds = RelationGetIndexList(rel);

		oldSlot = arg->scanSlot;
		newSlot = &arg->newSlot->base;
		Assert(oldSlot->tts_tupleDescriptor->natts == newSlot->tts_tupleDescriptor->natts);
		for (attnum = 0; attnum < oldSlot->tts_nvalid; attnum++)
		{
			Form_pg_attribute attr = &oldSlot->tts_tupleDescriptor->attrs[attnum];

			if ((oldSlot->tts_isnull[attnum] != newSlot->tts_isnull[attnum]) ||
				(!oldSlot->tts_isnull[attnum] &&
				 !datumIsEqual(oldSlot->tts_values[attnum], newSlot->tts_values[attnum],
							   attr->attbyval, attr->attlen)))
			{
				changed_attrs = bms_add_member(changed_attrs, attnum);
			}
		}

		if (oldSlot->tts_nvalid < newSlot->tts_nvalid)
		{
			/*
			 * This possible during update of rows that have nulls at the end.
			 * And during ExecModifyTable in ExecGetUpdateNewTuple it calls
			 * getsomeattrs with natts excluding last null values
			 */
			for (attnum = oldSlot->tts_nvalid; attnum < oldSlot->tts_tupleDescriptor->natts; attnum++)
			{
				 /* Assuming that tts_isnull big enough */ ;
				oldSlot->tts_isnull[attnum] = true;
				changed_attrs = bms_add_member(changed_attrs, attnum);
			}
		}

		foreach(indexId, indexIds)
		{
			Oid			indexOid = lfirst_oid(indexId);
			Relation	index_rel = index_open(indexOid, AccessExclusiveLock);
			bool		interesting = index_rel->rd_rel->relam != BTREE_AM_OID;

			if (!interesting)
			{
				OBTOptions *options = (OBTOptions *) index_rel->rd_options;

				interesting = options && !options->orioledb_index;
			}
			if (interesting)
			{
				for (attnum = 0; attnum < index_rel->rd_index->indnatts; attnum++)
				{
					AttrNumber	tbl_attnum = index_rel->rd_index->indkey.values[attnum];

					if (index_rel->rd_indpred != NIL)
					{
						ExprState  *predicate;
						EState	   *estate;
						ExprContext *econtext;

						estate = CreateExecutorState();
						predicate = ExecPrepareQual(index_rel->rd_indpred, estate);

						econtext = GetPerTupleExprContext(estate);
						econtext->ecxt_scantuple = newSlot;

						/*
						 * Skip this index-update if the predicate isn't
						 * satisfied
						 */
						if (!ExecQual(predicate, econtext))
						{
							FreeExecutorState(estate);
							continue;
						}
						FreeExecutorState(estate);
					}

					if (AttributeNumberIsValid(tbl_attnum))
					{
						if (bms_is_member(tbl_attnum - 1, changed_attrs))
							touched_indices = true;
					}
					else
					{
						Assert(false);	/* Expression indices not implemented
										 * yet. */
					}

					if (touched_indices)
						break;
				}
			}
			index_close(index_rel, AccessExclusiveLock);
		}
	}

	tts_orioledb_toast(slot, descr);
	tts_orioledb_fill_key_bound(slot, GET_PRIMARY(descr), &newPkey);
	if (touched_indices)
		o_apply_new_bridge_index_ctid(descr, rel, slot, csn);

	newTup = tts_orioledb_form_tuple(slot, descr);
	o_btree_check_size_of_tuple(o_tuple_size(newTup, &primary->leafSpec),
								RelationGetRelationName(rel),
								false);

	if (is_keys_eq(&GET_PRIMARY(descr)->desc, oldPkey, &newPkey))
	{
		mres = o_tbl_indices_overwrite(descr, &newPkey, slot, oxid, csn,
									   hint, arg);
	}
	else
	{
		mres = o_tbl_indices_reinsert(descr, oldPkey, &newPkey, slot,
									  oxid, csn, hint, arg);
	}
	csn = arg->csn;

	if (!arg->selfModified)
	{
		if (arg->deleted == BTreeLeafTupleMovedPartitions)
		{
			if (!IsolationUsesXactSnapshot())
				ereport(ERROR,
						(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
						 errmsg("tuple to be locked was already moved to another partition due to concurrent update")));
		}
		else if (arg->deleted == BTreeLeafTuplePKChanged)
		{
			if (!IsolationUsesXactSnapshot())
				ereport(ERROR,
						(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
						 errmsg("tuple to be locked has its primary key changed due to concurrent update")));
		}
	}

	if (mres.success)
		pgstat_count_heap_update(rel, false, false);

	if (mres.success && mres.oldTuple != NULL)
	{
		oldSlot = mres.oldTuple;

		if (mres.action == BTreeOperationUpdate)
		{
			if (touched_indices)
				delete_old_bridge_index_ctid(descr, rel, &((OTableSlot *) oldSlot)->bridge_ctid, csn);

			mres.failedIxNum = TOASTIndexNumber;
			mres.success = tts_orioledb_update_toast_values(oldSlot, slot, descr,
															oxid, csn);

			if (mres.success &&
				primary->desc.storageType == BTreeStoragePersistence)
			{
				OTuple		final_tup = tts_orioledb_form_tuple(slot, descr);

				elog(DEBUG3, "CALL o_wal_update");
				o_wal_update(&primary->desc, final_tup, ((OTableSlot *) oldSlot)->tuple, rel->rd_rel->relreplident);
			}
		}
		else if (mres.action == BTreeOperationDelete)
		{
			if (descr->bridge)
			{
				OTableSlot *oslot = (OTableSlot *) oldSlot;

				delete_old_bridge_index_ctid(descr, rel, &((OTableSlot *) oldSlot)->bridge_ctid, csn);
				reinsert_bridge_ctid_on_pkey_changed(descr, rel, slot, csn);
			}

			/* reinsert TOAST value */
			mres.failedIxNum = TOASTIndexNumber;
			/* insert new value in TOAST table */
			mres.success = tts_orioledb_insert_toast_values(slot, descr, oxid, csn);
			if (mres.success)
			{
				/* remove old value from TOAST table */
				mres.success = tts_orioledb_remove_toast_values(oldSlot, descr, oxid, csn);
			}

			if (mres.success &&
				primary->desc.storageType == BTreeStoragePersistence)
			{
				OTuple		final_tup = tts_orioledb_form_tuple(slot, descr);

				o_wal_reinsert(&primary->desc, ((OTableSlot *) oldSlot)->tuple, final_tup, rel->rd_rel->relreplident);
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
o_tbl_delete(Relation rel, OTableDescr *descr, OBTreeKeyBound *primary_key,
			 OXid oxid, CommitSeqNo csn,
			 BTreeLocationHint *hint, OModifyCallbackArg *arg)
{
	OTableModifyResult result;

	CheckCmdReplicaIdentity(rel, CMD_DELETE);

	result = o_tbl_indices_delete(descr, primary_key, oxid,
								  csn, hint, arg);

	if (!arg->selfModified)
	{
		if (arg->deleted == BTreeLeafTupleMovedPartitions)
		{
			if (!IsolationUsesXactSnapshot())
				ereport(ERROR,
						(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
						 errmsg("tuple to be locked was already moved to another partition due to concurrent update")));
		}
		else if (arg->deleted == BTreeLeafTuplePKChanged)
		{
			if (!IsolationUsesXactSnapshot())
				ereport(ERROR,
						(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
						 errmsg("tuple to be locked has its primary key changed due to concurrent update")));
		}
	}

	if (result.success)
		pgstat_count_heap_delete(rel);

	if (result.success && result.oldTuple != NULL)
	{
		if (result.action == BTreeOperationDelete)
		{
			OIndexDescr *primary = GET_PRIMARY(descr);
			OTuple		primary_tuple;
			OTableSlot *oslot = (OTableSlot *) result.oldTuple;

			csn = arg->csn;

			if (descr->bridge)
				delete_old_bridge_index_ctid(descr, rel, &oslot->bridge_ctid, csn);

			/* if tuple has been deleted from index trees, remove TOAST values */
			if (!tts_orioledb_remove_toast_values(result.oldTuple, descr, oxid, csn))
			{
				result.success = false;
				result.failedIxNum = TOASTIndexNumber;
				return result;
			}

			primary_tuple = ((OTableSlot *) result.oldTuple)->tuple;

			if (primary->desc.storageType == BTreeStoragePersistence)
				o_wal_delete(&primary->desc, primary_tuple, rel->rd_rel->relreplident);
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


/* fills key bound from tuple or index tuple that belongs to current BTree */
static void
fill_key_bound(TupleTableSlot *slot, OIndexDescr *idx, OBTreeKeyBound *bound)
{
	OTableSlot *oslot = (OTableSlot *) slot;
	int			i;

	slot_getallattrs(slot);

	bound->nkeys = idx->nonLeafTupdesc->natts;
	Assert(bound->nkeys > 0);	/* for clang-analyzer */
	for (i = 0; i < bound->nkeys; i++)
	{
		Datum		value;
		bool		isnull;
		Oid			typid;

		typid = idx->nonLeafTupdesc->attrs[i].atttypid;

		if (typid == TIDOID)
		{
			/*
			 * TODO: Do more complex check here, because it ignores ctid when
			 * bridging enabled
			 */
			if (idx->bridging &&
				(idx->desc.type == oIndexPrimary || idx->desc.type == oIndexBridge))
			{
				isnull = false;
				value = PointerGetDatum(&oslot->bridge_ctid);
			}
			else
			{
				isnull = false;
				value = PointerGetDatum(&slot->tts_tid);
			}
		}
		else
		{
			value = slot->tts_values[i];
			isnull = slot->tts_isnull[i];
		}

		bound->keys[i].value = value;
		bound->keys[i].type = typid;
		bound->keys[i].flags = O_VALUE_BOUND_PLAIN_VALUE;
		if (isnull)
			bound->keys[i].flags |= O_VALUE_BOUND_NULL;
		bound->keys[i].comparator = idx->fields[i].comparator;
		bound->keys[i].exclusion_fn = NULL;
	}
}

OTableModifyResult
o_update_secondary_index(OIndexDescr *id,
						 OIndexNumber ix_num,
						 bool new_valid,
						 bool old_valid,
						 TupleTableSlot *newSlot,
						 OTuple new_ix_tup,
						 TupleTableSlot *oldSlot,
						 OXid oxid,
						 CommitSeqNo csn,
						 IndexUniqueCheck checkUnique)
{
	OTableModifyResult res;
	OBTreeKeyBound old_key,
				new_key;
	OTuple		nullTup;
	BTreeModifyCallbackInfo callbackInfo = nullCallbackInfo;

	slot_getallattrs(oldSlot);

	memset(&res, 0, sizeof(res));
	res.success = true;
	res.oldTuple = oldSlot;

	fill_key_bound(oldSlot, id, &old_key);
	fill_key_bound(newSlot, id, &new_key);

	if (is_keys_eq(&id->desc, &old_key, &new_key) && (old_valid == new_valid))
		return res;

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

	if (!res.success)
	{
		res.action = BTreeOperationUpdate;
	}
	else if (new_valid)
	{
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
												NULL, &callbackInfo,
												checkUnique) == OBTreeModifyResultInserted;

		if (!res.success)
			res.action = BTreeOperationInsert;
	}
	if (!res.success)
		res.failedIxNum = ix_num;
	return res;
}

/* returns TupleTableSlot of old tuple as OTableModifyResul.result */
static OTableModifyResult
o_tbl_indices_overwrite(OTableDescr *descr,
						OBTreeKeyBound *oldPkey,
						TupleTableSlot *newSlot,
						OXid oxid, CommitSeqNo csn,
						BTreeLocationHint *hint,
						OModifyCallbackArg *arg)
{
	OTableModifyResult result;
	OTuple		newTup;
	OBTreeModifyResult modify_result;
	BTreeModifyCallbackInfo callbackInfo = {
		.waitCallback = NULL,
		.modifyDeletedCallback = o_update_deleted_callback,
		.modifyCallback = o_update_callback,
		.needsUndoForSelfCreated = false,
		.arg = arg
	};

	memset(&result, 0, sizeof(result));
	result.success = true;
	result.oldTuple = NULL;

	newTup = tts_orioledb_form_tuple(newSlot, descr);

	o_btree_load_shmem(&GET_PRIMARY(descr)->desc);

	modify_result = o_btree_modify(&GET_PRIMARY(descr)->desc, BTreeOperationUpdate,
								   newTup, BTreeKeyLeafTuple,
								   (Pointer) oldPkey, BTreeKeyBound,
								   oxid, csn, RowLockNoKeyUpdate,
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
	csn = arg->csn;

	if (modify_result == OBTreeModifyResultUpdated)
	{
		((OTableSlot *) newSlot)->version = o_tuple_get_version(newTup);
		if (result.success)
		{
			result.action = BTreeOperationUpdate;
			result.oldTuple = update_arg_get_slot(arg);
		}
	}
	else if (modify_result == OBTreeModifyResultFound ||
			 modify_result == OBTreeModifyResultNotFound)
	{
		/* primary key or condition was changed by concurrent transaction */
		result.success = true;
		result.oldTuple = NULL;
		result.action = BTreeOperationUpdate;
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
					   OXid oxid, CommitSeqNo csn,
					   BTreeLocationHint *hint, OModifyCallbackArg *arg)
{
	OTableModifyResult result;
	OBTreeModifyResult modify_result;
	OTuple		newTup;
	bool		inserted;
	BTreeModifyCallbackInfo deleteCallbackInfo = {
		.waitCallback = NULL,
		.modifyDeletedCallback = o_delete_deleted_callback,
		.modifyCallback = o_delete_callback,
		.needsUndoForSelfCreated = false,
		.arg = arg
	};
	BTreeModifyCallbackInfo insertCallbackInfo = {
		.waitCallback = NULL,
		.modifyDeletedCallback = o_insert_callback,
		.modifyCallback = NULL,
		.needsUndoForSelfCreated = false,
		.arg = newSlot
	};

	memset(&result, 0, sizeof(result));
	result.success = true;
	result.oldTuple = NULL;

	newTup = tts_orioledb_form_tuple(newSlot, descr);

	o_btree_load_shmem(&GET_PRIMARY(descr)->desc);

	modify_result = o_btree_delete_pk_changed(&GET_PRIMARY(descr)->desc,
											  (Pointer) oldPkey, BTreeKeyBound,
											  oxid, csn, hint,
											  &deleteCallbackInfo);

	if (modify_result == OBTreeModifyResultLocked)
	{
		Assert(arg->scanSlot);
		result.success = true;
		result.oldTuple = arg->scanSlot;
		result.action = BTreeOperationLock;
		return result;
	}
	else if (modify_result == OBTreeModifyResultNotFound)
	{
		result.success = true;
		result.oldTuple = NULL;
		result.action = BTreeOperationDelete;
		result.failedIxNum = PrimaryIndexNumber;
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
		result.success = true;
		result.oldTuple = update_arg_get_slot(arg);
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

OTableModifyResult
o_tbl_index_delete(OIndexDescr *id, OIndexNumber ix_num, TupleTableSlot *slot,
				   OXid oxid, CommitSeqNo csn)
{
	OTableModifyResult result;
	OBTreeModifyResult res;
	OModifyCallbackArg marg = {0};
	BTreeModifyCallbackInfo callbackInfo = {
		.waitCallback = NULL,
		.modifyDeletedCallback = o_delete_deleted_callback,
		.modifyCallback = o_delete_callback,
		.needsUndoForSelfCreated = false,
		.arg = &marg
	};
	OBTreeKeyBound bound;
	OTuple		nullTup;

	O_TUPLE_SET_NULL(nullTup);

	fill_key_bound(slot, id, &bound);
	o_btree_load_shmem(&id->desc);
	res = o_btree_modify(&id->desc, BTreeOperationDelete,
						 nullTup, BTreeKeyNone,
						 (Pointer) &bound, BTreeKeyBound,
						 oxid, csn, RowLockUpdate,
						 NULL, &callbackInfo);

	memset(&result, 0, sizeof(result));
	result.success = (res == OBTreeModifyResultDeleted) || marg.deleted;
	if (!result.success)
	{
		result.success = false;
		result.failedIxNum = ix_num;
	}
	return result;
}

/* Returns TupleTableSlot of old tuple as OTableModifyResult.result */
static OTableModifyResult
o_tbl_indices_delete(OTableDescr *descr, OBTreeKeyBound *key,
					 OXid oxid, CommitSeqNo csn, BTreeLocationHint *hint,
					 OModifyCallbackArg *arg)
{
	OTableModifyResult result;
	OBTreeModifyResult res;
	TupleTableSlot *slot;
	OTuple		nullTup;
	BTreeModifyCallbackInfo callbackInfo = {
		.waitCallback = NULL,
		.modifyDeletedCallback = o_delete_deleted_callback,
		.modifyCallback = o_delete_callback,
		.needsUndoForSelfCreated = false,
		.arg = arg
	};

	memset(&result, 0, sizeof(result));
	result.oldTuple = NULL;

	o_btree_load_shmem(&GET_PRIMARY(descr)->desc);
	O_TUPLE_SET_NULL(nullTup);

	if (!arg->changingPart)
		res = o_btree_modify(&GET_PRIMARY(descr)->desc, BTreeOperationDelete,
							 nullTup, BTreeKeyNone,
							 (Pointer) key, BTreeKeyBound,
							 oxid, csn, RowLockUpdate,
							 hint, &callbackInfo);
	else
		res = o_btree_delete_moved_partitions(&GET_PRIMARY(descr)->desc,
											  (Pointer) key, BTreeKeyBound,
											  oxid, csn, hint,
											  &callbackInfo);

	slot = update_arg_get_slot(arg);
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

	result.success = true;
	result.action = BTreeOperationDelete;
	result.oldTuple = slot;
	return result;
}

OBTreeModifyResult
o_tbl_index_insert(OTableDescr *descr,
				   OIndexDescr *id,
				   OTuple *own_tup,
				   TupleTableSlot *slot,
				   OXid oxid, CommitSeqNo csn,
				   BTreeModifyCallbackInfo *callbackInfo,
				   IndexUniqueCheck checkUnique)
{
	BTreeDescr *bd = &id->desc;
	OTuple		tup;
	OBTreeKeyBound knew;
	bool		primary = (bd->type == oIndexPrimary);

	OBTreeModifyResult result;

	if (!primary)
	{
		if (own_tup)
		{
			fill_key_bound(slot, id, &knew);
			tup = *own_tup;
		}
		else
		{
			tts_orioledb_fill_key_bound(slot, id, &knew);
			tup = tts_orioledb_make_secondary_tuple(slot, id, true);
		}
		o_btree_check_size_of_tuple(o_tuple_size(tup, &id->leafSpec),
									id->name.data, true);
	}
	else
	{
		tts_orioledb_fill_key_bound(slot, id, &knew);
		tup = tts_orioledb_form_tuple(slot, descr);
	}

	o_btree_load_shmem(bd);
	if (primary || !id->unique ||
		(!id->nulls_not_distinct && o_has_nulls(tup)))
		result = o_btree_modify(bd, BTreeOperationInsert,
								tup, BTreeKeyLeafTuple,
								(Pointer) &knew, BTreeKeyBound,
								oxid, csn, RowLockUpdate,
								NULL, callbackInfo);
	else
		result = o_btree_insert_unique(bd, tup, BTreeKeyLeafTuple,
									   (Pointer) &knew, BTreeKeyBound,
									   oxid, csn, RowLockUpdate,
									   NULL, callbackInfo, checkUnique);

	((OTableSlot *) slot)->version = o_tuple_get_version(tup);

	STOPEVENT(STOPEVENT_INDEX_INSERT, NULL);

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
				 errdetail("Unable to insert value for primary key %s into TOAST",
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
				  BTreeLeafTupleDeletedStatus deleted,
				  UndoLocation location, RowLockMode *lock_mode,
				  BTreeLocationHint *hint, void *arg)
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
o_insert_with_arbiter_wait_callback(BTreeDescr *descr,
									OTuple tup, OTuple *newtup,
									OXid oxid, OTupleXactInfo xactInfo,
									UndoLocation location,
									RowLockMode *lock_mode,
									BTreeLocationHint *hint,
									void *arg)
{
	InsertOnConflictCallbackArg *ioc_arg = (InsertOnConflictCallbackArg *) arg;

	if (descr->type == oIndexPrimary && ioc_arg->copyPrimaryOxid)
	{
		ioc_arg->conflictOxid = oxid;
		return OBTreeCallbackActionXidExit;
	}

	return OBTreeCallbackActionXidWait;
}

static OBTreeModifyCallbackAction
o_insert_with_arbiter_modify_deleted_callback(BTreeDescr *descr,
											  OTuple tup, OTuple *newtup,
											  OXid oxid,
											  OTupleXactInfo xactInfo,
											  BTreeLeafTupleDeletedStatus deleted,
											  UndoLocation location,
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
o_insert_with_arbiter_modify_callback(BTreeDescr *descr,
									  OTuple tup, OTuple *newtup,
									  OXid oxid, OTupleXactInfo xactInfo,
									  UndoLocation location,
									  RowLockMode *lock_mode,
									  BTreeLocationHint *hint,
									  void *arg)
{
	InsertOnConflictCallbackArg *ioc_arg = (InsertOnConflictCallbackArg *) arg;

	if (ioc_arg->scanSlot && ioc_arg->conflictIxNum != InvalidIndexNumber)
	{
		bool		modified;

		modified = o_callback_is_modified(ioc_arg->oxid, ioc_arg->csn, xactInfo);

		/* Updates current csn */
		if (XACT_INFO_IS_FINISHED(xactInfo))
		{
			ioc_arg->csn = modified ? (XACT_INFO_MAP_CSN(xactInfo) + 1) : ioc_arg->csn;
		}
		else
		{
			ioc_arg->csn = COMMITSEQNO_INPROGRESS;
			ioc_arg->oxid = XACT_INFO_GET_OXID(xactInfo);
			ioc_arg->tupUndoLocation = UndoLocationGetValue(location);
		}

		copy_tuple_to_slot(tup, ioc_arg->scanSlot, ioc_arg->desc,
						   ioc_arg->csn, ioc_arg->conflictIxNum, hint);

		if (ioc_arg->conflictIxNum == PrimaryIndexNumber)
		{
			*lock_mode = ioc_arg->lockMode;
			return OBTreeCallbackActionLock;
		}
	}

	return OBTreeCallbackActionDoNothing;
}

static OBTreeModifyCallbackAction
o_delete_callback(BTreeDescr *descr,
				  OTuple tup, OTuple *newtup,
				  OXid oxid, OTupleXactInfo xactInfo,
				  UndoLocation location, RowLockMode *lock_mode,
				  BTreeLocationHint *hint, void *arg)
{
	OModifyCallbackArg *o_arg = (OModifyCallbackArg *) arg;
	bool		modified;

	if (descr->type != oIndexPrimary)
		return OBTreeCallbackActionDelete;

	modified = o_callback_is_modified(o_arg->oxid, o_arg->csn, xactInfo);

	if (descr->type == oIndexPrimary &&
		XACT_INFO_OXID_IS_CURRENT(xactInfo))
	{
		o_arg->tupleCid = undo_location_get_command(UndoLocationGetValue(location));
		if (o_arg->tupleCid >= o_arg->modifyCid)
			o_arg->selfModified = true;
	}

	if (XACT_INFO_IS_FINISHED(xactInfo))
		o_arg->csn = modified ? (XACT_INFO_MAP_CSN(xactInfo) + 1) : o_arg->csn;
	else
	{
		o_arg->csn = COMMITSEQNO_INPROGRESS;
		o_arg->oxid = XACT_INFO_GET_OXID(xactInfo);
		o_arg->tup_undo_location = location;
	}

	o_arg->modified = modified;

	if (!modified || (o_arg->options & TABLE_MODIFY_LOCK_UPDATED))
	{
		copy_tuple_to_slot(tup, update_arg_get_slot(o_arg), o_arg->descr,
						   o_arg->csn, PrimaryIndexNumber, hint);
	}

	if (o_arg->selfModified)
		return OBTreeCallbackActionDoNothing;
	else if (!modified)
		return OBTreeCallbackActionDelete;
	else if (o_arg->options & TABLE_MODIFY_LOCK_UPDATED)
		return OBTreeCallbackActionLock;
	else
		return OBTreeCallbackActionDoNothing;
}

static OBTreeModifyCallbackAction
o_delete_deleted_callback(BTreeDescr *desc,
						  OTuple oldTup,
						  OTuple *newTup,
						  OXid oxid,
						  OTupleXactInfo xactInfo,
						  BTreeLeafTupleDeletedStatus deleted,
						  UndoLocation location,
						  RowLockMode *lockMode,
						  BTreeLocationHint *hint,
						  void *arg)
{
	OModifyCallbackArg *o_arg = (OModifyCallbackArg *) arg;
	bool		modified;

	o_arg->deleted = deleted;

	if (desc->type != oIndexPrimary)
		return OBTreeCallbackActionDelete;

	if (XACT_INFO_OXID_IS_CURRENT(xactInfo))
	{
		o_arg->tupleCid = undo_location_get_command(UndoLocationGetValue(location));
		if (o_arg->tupleCid >= o_arg->modifyCid)
			o_arg->selfModified = true;
	}

	modified = o_callback_is_modified(o_arg->oxid, o_arg->csn, xactInfo);

	if (XACT_INFO_IS_FINISHED(xactInfo))
		o_arg->csn = modified ? (XACT_INFO_MAP_CSN(xactInfo) + 1) : o_arg->csn;
	else
	{
		o_arg->csn = COMMITSEQNO_INPROGRESS;
		o_arg->oxid = XACT_INFO_GET_OXID(xactInfo);
		o_arg->tup_undo_location = location;
	}
	return OBTreeCallbackActionDoNothing;
}

static OBTreeModifyCallbackAction
o_update_callback(BTreeDescr *descr,
				  OTuple tup, OTuple *newtup,
				  OXid oxid, OTupleXactInfo xactInfo,
				  UndoLocation location,
				  RowLockMode *lock_mode,
				  BTreeLocationHint *hint, void *arg)
{
	OModifyCallbackArg *o_arg = (OModifyCallbackArg *) arg;
	TupleTableSlot *slot;
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

		o_arg->tupleCid = undo_location_get_command(UndoLocationGetValue(location));
		if (o_arg->tupleCid >= o_arg->modifyCid)
			o_arg->selfModified = true;
	}

	modified = o_callback_is_modified(o_arg->oxid, o_arg->csn, xactInfo);

	if (XACT_INFO_IS_FINISHED(xactInfo))
		o_arg->csn = modified ? (XACT_INFO_MAP_CSN(xactInfo) + 1) : o_arg->csn;
	else
	{
		o_arg->csn = COMMITSEQNO_INPROGRESS;
		o_arg->oxid = XACT_INFO_GET_OXID(xactInfo);
		o_arg->tup_undo_location = location;
	}

	o_arg->modified = modified;
	if (!modified || (o_arg->options & TABLE_MODIFY_LOCK_UPDATED))
	{
		slot = update_arg_get_slot(o_arg);
		copy_tuple_to_slot(tup, slot, o_arg->descr, o_arg->csn,
						   PrimaryIndexNumber, hint);
		if (tts_orioledb_modified(slot, &o_arg->newSlot->base, o_arg->keyAttrs))
			*lock_mode = RowLockUpdate;
		else
			*lock_mode = RowLockNoKeyUpdate;
	}

	if (o_arg->selfModified)
		return OBTreeCallbackActionDoNothing;
	else if (!modified)
		return OBTreeCallbackActionUpdate;

	if (o_arg->options & TABLE_MODIFY_LOCK_UPDATED)
		return OBTreeCallbackActionLock;
	else
		return OBTreeCallbackActionDoNothing;
}

static OBTreeModifyCallbackAction
o_update_deleted_callback(BTreeDescr *descr,
						  OTuple tup, OTuple *newtup,
						  OXid oxid, OTupleXactInfo xactInfo,
						  BTreeLeafTupleDeletedStatus deleted,
						  UndoLocation location,
						  RowLockMode *lock_mode,
						  BTreeLocationHint *hint, void *arg)
{
	OModifyCallbackArg *o_arg = (OModifyCallbackArg *) arg;
	bool		modified;

	o_arg->deleted = deleted;

	if (descr->type == oIndexPrimary &&
		XACT_INFO_OXID_IS_CURRENT(xactInfo))
	{
		o_arg->tupleCid = undo_location_get_command(UndoLocationGetValue(location));
		if (o_arg->tupleCid >= o_arg->modifyCid)
			o_arg->selfModified = true;
	}

	modified = o_callback_is_modified(o_arg->oxid, o_arg->csn, xactInfo);

	if (XACT_INFO_IS_FINISHED(xactInfo))
		o_arg->csn = modified ? (XACT_INFO_MAP_CSN(xactInfo) + 1) : o_arg->csn;
	else
	{
		o_arg->csn = COMMITSEQNO_INPROGRESS;
		o_arg->oxid = XACT_INFO_GET_OXID(xactInfo);
		o_arg->tup_undo_location = location;
	}

	return OBTreeCallbackActionDoNothing;
}

static OBTreeWaitCallbackAction
o_lock_wait_callback(BTreeDescr *descr, OTuple tup, OTuple *newtup,
					 OXid oxid, OTupleXactInfo xactInfo, UndoLocation location,
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
			/* cppcheck-suppress missingReturn */
			break;
		default:
			elog(ERROR, "Unknown wait policy: %u", o_arg->waitPolicy);
			break;
	}
}

static OBTreeModifyCallbackAction
o_lock_modify_callback(BTreeDescr *descr, OTuple tup, OTuple *newtup,
					   OXid oxid, OTupleXactInfo xactInfo,
					   UndoLocation location,
					   RowLockMode *lock_mode, BTreeLocationHint *hint,
					   void *arg)
{
	OLockCallbackArg *o_arg = (OLockCallbackArg *) arg;
	TupleTableSlot *slot = o_arg->scanSlot;

	o_arg->modified = o_callback_is_modified(o_arg->oxid, o_arg->csn, xactInfo);

	Assert(descr->type == oIndexPrimary);

	if (XACT_INFO_OXID_IS_CURRENT(xactInfo))
	{
		o_arg->tupleCid = undo_location_get_command(UndoLocationGetValue(location));
		if (o_arg->tupleCid >= o_arg->modifyCid)
			o_arg->selfModified = true;
	}

	if (XACT_INFO_IS_FINISHED(xactInfo))
	{
		/*
		 * modified here means that tuple was modified, but current lock is
		 * weaker so it uses original tuple
		 */
		if (o_arg->modified)
		{
			CommitSeqNo csn = XACT_INFO_MAP_CSN(xactInfo);

			if (COMMITSEQNO_IS_NORMAL(csn))
				o_arg->csn = (csn + 1);
		}
	}
	else
	{
		o_arg->csn = COMMITSEQNO_INPROGRESS;
		o_arg->oxid = XACT_INFO_GET_OXID(xactInfo);
		o_arg->tupUndoLocation = UndoLocationGetValue(location);
	}

	copy_tuple_to_slot(tup, slot, o_arg->descr, o_arg->csn,
					   PrimaryIndexNumber, hint);

	return OBTreeCallbackActionLock;
}

static OBTreeModifyCallbackAction
o_lock_deleted_callback(BTreeDescr *descr,
						OTuple tup, OTuple *newtup,
						OXid oxid, OTupleXactInfo xactInfo,
						BTreeLeafTupleDeletedStatus deleted,
						UndoLocation location,
						RowLockMode *lock_mode,
						BTreeLocationHint *hint, void *arg)
{
	OLockCallbackArg *o_arg = (OLockCallbackArg *) arg;
	bool		modified;

	modified = o_callback_is_modified(o_arg->oxid, o_arg->csn, xactInfo);

	o_arg->deleted = deleted;

	if (XACT_INFO_IS_FINISHED(xactInfo))
	{
		o_arg->csn = modified ? (XACT_INFO_MAP_CSN(xactInfo) + 1) : o_arg->csn;
	}
	else
	{
		o_arg->csn = COMMITSEQNO_INPROGRESS;
		o_arg->oxid = XACT_INFO_GET_OXID(xactInfo);
		o_arg->tupUndoLocation = UndoLocationGetValue(location);
	}

	if (deleted == BTreeLeafTupleMovedPartitions)
		ereport(ERROR,
				(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
				 errmsg("tuple to be locked was already moved to another partition due to concurrent update")));
	else if (deleted == BTreeLeafTuplePKChanged)
		ereport(ERROR,
				(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
				 errmsg("tuple to be locked has its primary key changed due to concurrent update")));

	return OBTreeCallbackActionDoNothing;
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
	bool		is_ctid = id->primaryIsCtid;
	bool		is_primary = id->desc.type == oIndexPrimary;

	if (is_primary && is_ctid)
	{
		if (((OTableSlot *) slot)->tuple.data)
			pfree(((OTableSlot *) slot)->tuple.data);

		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("ctid index key duplicate.")));
	}
	else
	{
		StringInfo	str = makeStringInfo();
		int			i;

		appendStringInfo(str, "(");
		for (i = 0; i < id->nKeyFields; i++)
		{
			if (i != 0)
				appendStringInfo(str, ", ");
			appendStringInfo(str, "%s",
							 id->nonLeafTupdesc->attrs[i].attname.data);
		}
		appendStringInfo(str, ")=");
		appendStringInfoIndexKey(str, slot, id);
		if (((OTableSlot *) slot)->tuple.data)
			pfree(((OTableSlot *) slot)->tuple.data);
		ereport(ERROR,
				(errcode(ERRCODE_UNIQUE_VIOLATION),
				 errmsg("duplicate key value violates unique "
						"constraint \"%s\"", id->name.data),
				 errdetail("Key %s already exists.", str->data),
				 errtableconstraint(rel, id->desc.type == oIndexPrimary ?
									"pk" : "sk")));
	}
}

void
o_truncate_table(ORelOids oids, bool missingOK)
{
	ORelOids   *treeOids;
	OTable	   *o_table;
	int			treeOidsNum;
	int			i;
	bool		invalidatedTable = false;
	bool		is_temp;

	o_tables_rel_lock(&oids, AccessExclusiveLock);

	o_table = o_tables_get(oids);
	if (o_table == NULL)
	{
		if (!missingOK)
		{
			Assert(o_table != NULL);
			elog(ERROR, "o_truncate_table() missing table for oids (%u, %u, %u)",
				 oids.datoid, oids.reloid, oids.relnode);
		}
		else
		{
			return;
		}
	}
	is_temp = o_table->persistence == RELPERSISTENCE_TEMP;

	treeOids = o_table_make_index_oids(o_table, &treeOidsNum);

	for (i = 0; i < treeOidsNum; i++)
	{
		o_tables_rel_lock_extended(&treeOids[i], AccessExclusiveLock, false);
		o_tables_rel_lock_extended(&treeOids[i], AccessExclusiveLock, true);
		cleanup_btree(treeOids[i].datoid, treeOids[i].relnode, true, !is_temp);
		o_invalidate_oids(treeOids[i]);
/*		if (is_recovery_process())
			o_invalidate_descrs(treeOids[i].datoid, treeOids[i].reloid,
								treeOids[i].relnode);*/
		if (ORelOidsIsEqual(oids, treeOids[i]))
			invalidatedTable = true;
		o_tables_rel_unlock_extended(&treeOids[i], AccessExclusiveLock, false);
		o_tables_rel_unlock_extended(&treeOids[i], AccessExclusiveLock, true);
	}

	if (!invalidatedTable)
	{
		cleanup_btree(oids.datoid, oids.relnode, true, !is_temp);
		o_invalidate_oids(oids);
/*		if (is_recovery_process())
			o_invalidate_descrs(oids.datoid, oids.reloid, oids.relnode);*/
	}

	o_tables_rel_unlock(&oids, AccessExclusiveLock);

	pfree(treeOids);
}

Datum
orioledb_int4range_immutable(PG_FUNCTION_ARGS)
{
	char	   *range_input = text_to_cstring(PG_GETARG_TEXT_PP(0));
	Datum		range;

	range = OidInputFunctionCall(F_RANGE_IN, range_input,
								 INT4RANGEOID, -1);
	PG_RETURN_DATUM(range);
}
