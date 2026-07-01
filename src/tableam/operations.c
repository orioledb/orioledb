/*-------------------------------------------------------------------------
 *
 * operations.c
 *		Implementation of table-level operations
 *
 * Copyright (c) 2021-2026, Oriole DB Inc.
 * Copyright (c) 2025-2026, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/tableam/operations.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "orioledb.h"

#include "btree/btree.h"
#include "btree/find.h"
#include "btree/insert.h"
#include "btree/iterator.h"
#include "btree/modify.h"
#include "btree/undo.h"
#include "utils/page_pool.h"
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
#if PG_VERSION_NUM >= 180000
#include "replication/conflict.h"
#include "replication/worker_internal.h"
#endif
#include "storage/bufmgr.h"
#include "utils/datum.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"

static void set_pending_sk_marker_from_slot(UndoLocation pkUndoLoc, void *arg);
static void set_pending_sk_marker_from_modify_arg(UndoLocation pkUndoLoc,
												  void *arg);
static int	o_exclusion_cmp(OIndexDescr *id, OBTreeKeyBound *key1, OTuple *tuple2);

/*
 * Set ODBProcData.pendingSkUndoLoc to mark the PK-applied/SK-pending
 * window so that any checkpointer scan in between sees this row's
 * undo location.  Fires STOPEVENT_SK_MODIFY_PENDING immediately after,
 * which deterministic tests use to interleave a CHECKPOINT in the
 * window.  No-op when the PK btree does not produce a regular undo
 * entry (toast/sys trees, in-progress reads, etc.).
 *
 * Used both by the normal DML path and by recovery workers replaying
 * WAL on the replica, so a restartpoint observes the same window.
 */
/*
 * Convenience postUndoRecorded callbacks that extract the OTableDescr from
 * whatever arg shape the caller is already passing to the other callbacks
 * in BTreeModifyCallbackInfo.  Each call site picks the variant that
 * matches its `arg`.
 */
static void
set_pending_sk_marker_from_slot(UndoLocation pkUndoLoc, void *arg)
{
	set_pending_sk_marker(((OTableSlot *) arg)->descr, pkUndoLoc);
}

static void
set_pending_sk_marker_from_modify_arg(UndoLocation pkUndoLoc, void *arg)
{
	set_pending_sk_marker(((OModifyCallbackArg *) arg)->descr, pkUndoLoc);
}

void
set_pending_sk_marker(OTableDescr *descr, UndoLocation pkUndoLoc)
{
	if (GET_PRIMARY(descr)->desc.undoType != UndoLogRegular)
		return;

	/*
	 * No secondary index, no PK/SK desynchronisation risk -- skip the marker
	 * entirely.  This also avoids leaking a WaitingSkUndoLoc sentinel from
	 * code paths that call table_tuple_insert() without later invoking
	 * table_tuple_complete_modification() (CREATE TABLE AS, REFRESH MAT VIEW,
	 * COPY into a fresh table without SK, ...).
	 */
	if (descr->nIndices < 2)
		return;

	/*
	 * Two acceptable inputs: a real undo location (regular path) or the
	 * WaitingSkUndoLoc sentinel that the PK btree_modify produced for a
	 * self-created table.  Anything else means the PK modification didn't
	 * happen or didn't produce trackable state.
	 */
	if (!UndoLocationIsValid(pkUndoLoc) && pkUndoLoc != WaitingSkUndoLoc)
		return;

	pg_atomic_write_u64(&GET_CUR_PROCDATA()->pendingSkUndoLoc, pkUndoLoc);
}

/*
 * Fire STOPEVENT_SK_MODIFY_PENDING after o_btree_modify has returned, so
 * deterministic tests can park here OUTSIDE any page lock.  No-op when the
 * marker was not actually installed for this proc (e.g. PK btree had no
 * undo, table has no SK, or the modify did not happen).
 */
void
fire_sk_modify_pending_stopevent(OTableDescr *descr)
{
	UndoLocation cur;

	if (!STOPEVENTS_ENABLED())
		return;
	if (GET_PRIMARY(descr)->desc.undoType != UndoLogRegular)
		return;
	if (descr->nIndices < 2)
		return;

	cur = pg_atomic_read_u64(&GET_CUR_PROCDATA()->pendingSkUndoLoc);
	if (!UndoLocationIsValid(cur) && cur != WaitingSkUndoLoc)
		return;

	{
		JsonbParseState *state = NULL;
		Jsonb	   *params;
		MemoryContext mctx = MemoryContextSwitchTo(stopevents_cxt);

		pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
		btree_desc_stopevent_params_internal(&GET_PRIMARY(descr)->desc, &state);
		params = JsonbValueToJsonb(pushJsonbValue(&state, WJB_END_OBJECT, NULL));
		MemoryContextSwitchTo(mctx);
		STOPEVENT(STOPEVENT_SK_MODIFY_PENDING, params);
	}
}

void
clear_pending_sk_marker(void)
{
	pg_atomic_write_u64(&GET_CUR_PROCDATA()->pendingSkUndoLoc,
						InvalidUndoLocation);
}

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
static inline bool is_keys_eq(OIndexDescr *id, OBTreeKeyBound *k1, OBTreeKeyBound *k2);
static void o_report_duplicate(Relation rel, OIndexDescr *id,
							   TupleTableSlot *slot);

/*
 * If we're inside a logical replication apply (or tablesync) worker, bump
 * pg_stat_subscription_stats.confl_* the same way upstream's
 * CheckAndReportConflict path does for heap tables.  Without this the
 * counter stays at 0 because orioledb's tuple_insert raises the unique
 * violation directly, bypassing ExecInsertIndexTuples and
 * CheckAndReportConflict.
 */
#if PG_VERSION_NUM >= 180000
static inline void
o_report_apply_conflict(ConflictType type)
{
	if (MySubscription)
		pgstat_report_subscription_conflict(MySubscription->oid, type);
}
#else
#define o_report_apply_conflict(type)	((void) 0)
#endif

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
							  TupleTableSlot *slot, CommitSeqNo csn, bool increment_bridge_ctid)
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
		if (increment_bridge_ctid)
		{
			o_btree_load_shmem(&primary->desc);
			oslot->bridge_ctid = btree_bridge_ctid_get_and_inc(&primary->desc, &overflow);
			oslot->bridgeChanged = true;
		}

		values[0] = PointerGetDatum(&oslot->bridge_ctid);
		isnull[0] = false;

		tuple = o_form_tuple(descr->bridge->leafTupdesc, &descr->bridge->leafSpec, version,
							 values, isnull, NULL);
		bridge_slot = descr->bridge->new_leaf_slot;
		tts_orioledb_store_tuple(bridge_slot, tuple, descr, csn, BridgeIndexNumber, false, NULL);
		callbackInfo.arg = bridge_slot;

		fill_current_oxid_osnapshot(&oxid, &o_snapshot);

		success = (o_tbl_index_insert(descr, descr->bridge, &tuple, bridge_slot,
									  oxid, o_snapshot.csn, &callbackInfo,
									  UNIQUE_CHECK_YES) == OBTreeModifyResultInserted);

		if (!success && !overflow)
			o_report_duplicate(relation, descr->bridge, bridge_slot);
	} while (!success);

	if (primary->desc.storageType == BTreeStoragePersistence)
	{
		o_wal_insert(&descr->bridge->desc, tuple, REPLICA_IDENTITY_DEFAULT, descr->version);
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
		o_wal_delete_key(&descr->bridge->desc, keyTuple, true, descr->version);
		flush_local_wal(false, false);
	}

	if (!result.success)
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("Couldn't delete old bridge ctid: %s",
							   tss_orioledb_print_idx_key(bridge_slot, descr->bridge))));
}

TupleTableSlot *
o_tbl_insert(OTableDescr *descr, Relation relation,
			 TupleTableSlot *slot, OXid oxid, CommitSeqNo csn)
{
	OTableModifyResult mres;
	OTuple		tup;
	OIndexDescr *primary = GET_PRIMARY(descr);
	bool		was_saving;
	BTreeModifyCallbackInfo callbackInfo =
	{
		.waitCallback = NULL,
		.modifyDeletedCallback = o_insert_callback,
		.modifyCallback = NULL,
		.needsUndoForSelfCreated = false,
		.postUndoRecorded = set_pending_sk_marker_from_slot
	};

	was_saving = o_start_saving_inval_messages();
	CheckCmdReplicaIdentity(relation, CMD_INSERT);
	o_stop_saving_inval_messages(was_saving);

	if (slot->tts_ops != descr->newTuple->tts_ops ||
		(((OTableSlot *) slot)->descr != NULL &&
		 ((OTableSlot *) slot)->descr != descr))
	{
		((OTableSlot *) descr->newTuple)->descr = descr;
		ExecCopySlot(descr->newTuple, slot);
		slot = descr->newTuple;
	}

	/*
	 * Wire .arg only after the slot may have been swapped to descr->newTuple
	 * above -- both o_insert_callback and the post-undo hook read
	 * ((OTableSlot *) arg)->descr, which is only valid on an orioledb slot.
	 */
	callbackInfo.arg = slot;

	if (GET_PRIMARY(descr)->primaryIsCtid)
	{
		ItemPointerData iptr;

		o_btree_load_shmem(&primary->desc);
		iptr = btree_ctid_get_and_inc(&primary->desc);
		tts_orioledb_set_ctid(slot, &iptr);
	}

	if (descr->bridge)
		o_apply_new_bridge_index_ctid(descr, relation, slot, csn, true);

	tts_orioledb_toast(slot, descr);

	tup = tts_orioledb_form_tuple(slot, descr);
	o_btree_check_size_of_tuple(o_tuple_size(tup, &primary->leafSpec),
								RelationGetRelationName(relation),
								false);

	mres.success = (o_tbl_index_insert(descr, descr->indices[0], NULL, slot,
									   oxid, csn, &callbackInfo,
									   UNIQUE_CHECK_YES) == OBTreeModifyResultInserted);

	/*
	 * The marker (if any) was already installed under page lock by the
	 * postUndoRecorded hook; here we just fire the stopevent outside of any
	 * page lock so deterministic tests can park here without blocking
	 * concurrent backends on the same leaf.
	 */
	fire_sk_modify_pending_stopevent(descr);
	if (!mres.success)
	{
		mres.failedIxNum = 0;
		mres.action = BTreeOperationInsert;
		mres.oldTuple = NULL;

		o_report_apply_conflict(CT_INSERT_EXISTS);
		o_report_duplicate(relation, descr->indices[mres.failedIxNum], slot);
	}
	else
	{
		pgstat_count_heap_insert(relation, 1);
	}

	o_toast_insert_values(relation, descr, slot, oxid, csn);

	/* Tuple might be changed in the callback */
	tup = tts_orioledb_form_tuple(slot, descr);

	if (primary->desc.storageType == BTreeStoragePersistence)
		o_wal_insert(&primary->desc, tup, relation->rd_rel->relreplident, descr->version);

	return slot;
}

/*
 * Comparator for qsort_arg'ing the permutation array idx[] used by
 * o_tbl_multi_insert when input keys are not monotone.  Sorts indices
 * by the key bound they refer to.
 */
typedef struct MultiInsertSortCtx
{
	BTreeDescr *desc;
	OBTreeKeyBound *keys;
} MultiInsertSortCtx;

static int
multi_insert_sort_cmp(const void *a, const void *b, void *arg)
{
	MultiInsertSortCtx *cx = (MultiInsertSortCtx *) arg;
	int			ia = *(const int *) a;
	int			ib = *(const int *) b;

	return o_btree_cmp(cx->desc,
					   (Pointer) &cx->keys[ia], BTreeKeyBound,
					   (Pointer) &cx->keys[ib], BTreeKeyBound);
}

/*
 * Multi-row insert with same-leaf batching for the primary index.
 *
 * Phase 1: per-slot primary tuple, key bound, ctid + bridge ctid, in-row
 *          TOAST.
 *
 * Phase 2: optimistically assume keys[] are monotone and just verify with
 *          an O(n) scan; the common cases (CTID-PK by construction,
 *          ordered explicit-PK COPY) hit this fast path and reuse the
 *          original arrays in place.  Only if the check fails do we fall
 *          back to building an idx[] permutation, qsort_arg-ing by key,
 *          and materializing sorted views of tuples / tuplens / keyptrs /
 *          cb_args.  The caller's slots[] stays in arrival order so
 *          copyfrom.c's linenos[] indexing remains valid.  Sorting is
 *          required because the leaf probe detects "key past this leaf's
 *          hikey" but not "key before this leaf's lokey", so non-monotone
 *          input could corrupt downlinks.
 *
 * Phase 3: stream sorted keys through primary leaves, holding each leaf's
 *          lwlock for as many adjacent keys as fit
 *          (o_btree_multi_insert_item).  Each iteration tops up row-undo
 *          for the upcoming batch, capped at 2 * O_MAX_UNDO_RECORD_SIZE
 *          so max_procs concurrent multi_inserts can't outrun the row
 *          buffer; larger inputs split across iterations.  HikeyCrossed
 *          re-finds the next leaf; NoFit / Duplicate slow-paths one row
 *          via o_tbl_index_insert and resumes.
 *
 * Phase 4: per-slot TOAST values insert + WAL.
 *
 * Slots that aren't already orioledb-typed share a single descr->newTuple
 * scratch slot and can't be batched -- in that case fall back to per-row
 * o_tbl_insert before doing any work.
 */
void
o_tbl_multi_insert(OTableDescr *descr, Relation relation,
				   TupleTableSlot **slots, int ntuples,
				   OXid oxid, CommitSeqNo csn)
{
	OIndexDescr *primary = GET_PRIMARY(descr);
	BTreeDescr *pdesc = &primary->desc;
	bool		was_saving;
	OTuple	   *tuples;
	LocationIndex *tuplens;
	OBTreeKeyBound *keys;
	Pointer    *keyptrs;
	OBTreeFindPageContext ctx;
	BTreeModifyCallbackInfo callbackInfo =
	{
		.waitCallback = NULL,
		.modifyDeletedCallback = o_insert_callback,
		.modifyCallback = NULL,
		.needsUndoForSelfCreated = false,
		.postUndoRecorded = set_pending_sk_marker_from_slot
	};
	int			i;

	was_saving = o_start_saving_inval_messages();
	CheckCmdReplicaIdentity(relation, CMD_INSERT);
	o_stop_saving_inval_messages(was_saving);

	o_btree_load_shmem(pdesc);

	/*
	 * Non-orioledb slots share descr->newTuple; can't hold N independent
	 * pre-formed tuples across Phase 3.  Fall back to per-row.
	 */
	for (i = 0; i < ntuples; i++)
	{
		TupleTableSlot *slot = slots[i];

		if (slot->tts_ops != descr->newTuple->tts_ops ||
			(((OTableSlot *) slot)->descr != NULL &&
			 ((OTableSlot *) slot)->descr != descr))
		{
			for (i = 0; i < ntuples; i++)
				o_tbl_insert(descr, relation, slots[i], oxid, csn);
			return;
		}
	}

	tuples = (OTuple *) palloc(sizeof(OTuple) * ntuples);
	tuplens = (LocationIndex *) palloc(sizeof(LocationIndex) * ntuples);
	keys = (OBTreeKeyBound *) palloc(sizeof(OBTreeKeyBound) * ntuples);
	keyptrs = (Pointer *) palloc(sizeof(Pointer) * ntuples);

	/* Phase 1: per-slot prep (ctid, bridge, toast, form, key bound). */
	for (i = 0; i < ntuples; i++)
	{
		TupleTableSlot *slot = slots[i];

		if (primary->primaryIsCtid)
		{
			ItemPointerData iptr = btree_ctid_get_and_inc(pdesc);

			tts_orioledb_set_ctid(slot, &iptr);
		}

		if (descr->bridge)
			o_apply_new_bridge_index_ctid(descr, relation, slot, csn, true);

		tts_orioledb_toast(slot, descr);

		tuples[i] = tts_orioledb_form_tuple(slot, descr);
		tuplens[i] = o_tuple_size(tuples[i], &primary->leafSpec);
		o_btree_check_size_of_tuple(tuplens[i],
									RelationGetRelationName(relation), false);

		tts_orioledb_fill_key_bound(slot, primary, &keys[i]);
		keyptrs[i] = (Pointer) &keys[i];
	}

	/*
	 * Phase 2: the batch helper assumes keys[] ascend (its probe detects
	 * "past hikey" but not "before lokey" -- lokey lives in the parent, not
	 * the leaf, so a key < lokey would silently corrupt the downlink
	 * invariant).  CTID-PK input is monotone by construction (Phase 1's
	 * btree_ctid_get_and_inc); explicit-PK COPY may arrive unsorted.
	 *
	 * Optimistically assume monotone and just verify with an O(n) scan; the
	 * common cases pass and Phase 3 consumes the original arrays in place. On
	 * the first out-of-order pair fall back to sorting: build an idx[]
	 * permutation, qsort it by key, and materialise sorted views of the
	 * parallel arrays.  slots[] itself stays in arrival order so the caller's
	 * linenos[] indexing (copyfrom.c) remains correct; the sorted view's
	 * cb_args[] and the post-insert bookkeeping resolve back to the original
	 * slot via idx[].
	 */
	{
		OTuple	   *use_tuples = tuples;
		LocationIndex *use_tuplens = tuplens;
		Pointer    *use_keyptrs = keyptrs;
		void	  **use_cb_args = (void **) slots;
		int		   *idx = NULL;
		bool		sorted = true;

		for (i = 1; i < ntuples; i++)
		{
			if (o_btree_cmp(pdesc, keyptrs[i - 1], BTreeKeyBound,
							keyptrs[i], BTreeKeyBound) > 0)
			{
				sorted = false;
				break;
			}
		}

		if (!sorted)
		{
			MultiInsertSortCtx sortcx = {pdesc, keys};
			OTuple	   *sorted_tuples;
			LocationIndex *sorted_tuplens;
			Pointer    *sorted_keyptrs;
			void	  **sorted_cb_args;

			idx = (int *) palloc(sizeof(int) * ntuples);
			for (i = 0; i < ntuples; i++)
				idx[i] = i;
			qsort_arg(idx, ntuples, sizeof(int),
					  multi_insert_sort_cmp, &sortcx);

			sorted_tuples = (OTuple *) palloc(sizeof(OTuple) * ntuples);
			sorted_tuplens = (LocationIndex *) palloc(sizeof(LocationIndex) * ntuples);
			sorted_keyptrs = (Pointer *) palloc(sizeof(Pointer) * ntuples);
			sorted_cb_args = (void **) palloc(sizeof(void *) * ntuples);
			for (i = 0; i < ntuples; i++)
			{
				sorted_tuples[i] = tuples[idx[i]];
				sorted_tuplens[i] = tuplens[idx[i]];
				sorted_keyptrs[i] = (Pointer) &keys[idx[i]];
				sorted_cb_args[i] = slots[idx[i]];
			}
			use_tuples = sorted_tuples;
			use_tuplens = sorted_tuplens;
			use_keyptrs = sorted_keyptrs;
			use_cb_args = sorted_cb_args;
		}

		/* Phase 3: drain into primary leaves. */
		init_page_find_context(&ctx, pdesc, COMMITSEQNO_INPROGRESS,
							   BTREE_PAGE_FIND_MODIFY | BTREE_PAGE_FIND_FIX_LEAF_SPLIT);

		i = 0;
		while (i < ntuples)
		{
			OFindPageResult fr PG_USED_FOR_ASSERTS_ONLY;
			BTreeLeafProbeResult result;
			int			n;
			int			remaining = ntuples - i;
			int			batch = remaining;
			int			k;
			int			orig;

			if (pdesc->undoType != UndoLogNone)
			{
				Size		need = MAXIMUM_ALIGNOF;
				Size		maxrow = 0;

				/*
				 * Bound the batch by the per-backend row-undo share the
				 * circular buffer is sized for (see undo_shmem_needs); larger
				 * inputs are processed in successive chunks.  The trailing
				 * maxrow slot absorbs the one extra `size` that
				 * get_undo_record may consume on a buffer-wrap retry.
				 */
				for (k = 0; k < remaining; k++)
				{
					Size		one = MAXALIGN(sizeof(BTreeModifyUndoStackItem) + use_tuplens[i + k]);

					if (k > 0 && need + one + Max(maxrow, one) > 2 * O_MAX_UNDO_RECORD_SIZE)
						break;
					need += one;
					if (one > maxrow)
						maxrow = one;
				}
				batch = k;
				need += maxrow;
				reserve_undo_size(pdesc->undoType, need);
			}
			ppool_reserve_pages(pdesc->ppool, PPOOL_RESERVE_INSERT, 2);

			fr = find_page(&ctx, use_keyptrs[i], BTreeKeyBound, 0);
			Assert(fr == OFindPageResultSuccess);

			n = o_btree_multi_insert_item(&ctx,
										  use_tuples + i, use_tuplens + i,
										  use_keyptrs + i, BTreeKeyBound,
										  batch,
										  oxid, RowLockUpdate,
										  &callbackInfo,
										  use_cb_args + i,
										  &result);

			for (k = 0; k < n; k++)
			{
				orig = idx ? idx[i + k] : i + k;
				((OTableSlot *) slots[orig])->version = o_tuple_get_version(use_tuples[i + k]);
				pgstat_count_heap_insert(relation, 1);
				fire_sk_modify_pending_stopevent(descr);
			}
			i += n;

			if (i >= ntuples)
				break;

			/*
			 * HikeyCrossed -> re-find the next leaf; Fits with i < ntuples
			 * means the helper exited because the per-batch undo cap was
			 * reached, not because of a bail condition -- just re-reserve and
			 * continue.  Slow path runs only on NoFit / Duplicate.
			 */
			if (result == BTreeLeafProbeHikeyCrossed ||
				result == BTreeLeafProbeFits)
				continue;

			/*
			 * Slow path for one item.  Resolve back to the original slot so
			 * o_report_duplicate and the post-undo callback see the row the
			 * caller submitted, not the sorted-position alias.
			 */
			orig = idx ? idx[i] : i;
			callbackInfo.arg = slots[orig];
			if (o_tbl_index_insert(descr, primary, &tuples[orig], slots[orig],
								   oxid, csn, &callbackInfo,
								   UNIQUE_CHECK_YES) != OBTreeModifyResultInserted)
			{
				o_report_apply_conflict(CT_INSERT_EXISTS);
				o_report_duplicate(relation, primary, slots[orig]);
			}
			else
			{
				pgstat_count_heap_insert(relation, 1);
			}
			fire_sk_modify_pending_stopevent(descr);
			i++;
		}
	}

	/*
	 * Release any reservation still held (idempotent if the last iteration
	 * slow-pathed and the modify already released).
	 */
	if (pdesc->undoType != UndoLogNone)
		release_undo_size(pdesc->undoType);
	ppool_release_reserved(pdesc->ppool, PPOOL_RESERVE_INSERT_MASK);

	/* Phase 4: per-slot TOAST values + WAL. */
	for (i = 0; i < ntuples; i++)
	{
		TupleTableSlot *slot = slots[i];
		OTuple		tup;

		o_toast_insert_values(relation, descr, slot, oxid, csn);
		tup = tts_orioledb_form_tuple(slot, descr);

		if (pdesc->storageType == BTreeStoragePersistence)
			o_wal_insert(pdesc, tup, relation->rd_rel->relreplident,
						 descr->version);
	}

	pfree(tuples);
	pfree(tuplens);
	pfree(keys);
	pfree(keyptrs);
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
			pkey->keys[i].type = TupleDescAttr(idx->leafTupdesc, pk_from + i)->atttypid;
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
			pkey->keys[i].type = TupleDescAttr(primary->leafTupdesc, attnum - 1)->atttypid;
			pkey->keys[i].flags = O_VALUE_BOUND_PLAIN_VALUE;
			if (slot->tts_isnull[attnum - 1])
				pkey->keys[i].flags |= O_VALUE_BOUND_NULL;
			pkey->keys[i].comparator = primary->fields[attnum - 1].comparator;
			pkey->keys[i].exclusion_fn = NULL;
		}
	}
}

static int
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
								  &isnull, &typid, true);
		else
		{
			value = o_get_idx_expr_att(slot, idx,
									   (ExprState *) lfirst(indexpr_item),
									   &isnull);
			typid = TupleDescAttr(idx->nonLeafTupdesc, i)->atttypid;
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
			{
				pfree(tuple.data);
				btree_iterator_free(iter);
				return false;
			}
		}

		pfree(tuple.data);
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
	Datum		conflictRowid = PointerGetDatum((void *) 0xB0B);

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
			Datum	   *conflictRowidPtr = &conflictRowid;
			Datum		conflictRowidPtrDatum = PointerGetDatum(conflictRowidPtr);

#if PG_VERSION_NUM >= 180000
			ItemPointerData invalidItemPtr;
			Datum		invalidItemPtrDatum;

			if (table_get_row_ref_type(resultRelInfo->ri_RelationDesc) == ROW_REF_ROWID)
				invalidItemPtrDatum = PointerGetDatum(NULL);
			else
			{
				ItemPointerSetInvalid(&invalidItemPtr);
				invalidItemPtrDatum = ItemPointerGetDatum(&invalidItemPtr);
			}

			if (!ExecCheckIndexConstraints(resultRelInfo, slot, estate,
										   conflictRowidPtrDatum,
										   invalidItemPtrDatum, arbiterIndexes))
#else
			if (!ExecCheckIndexConstraints(resultRelInfo, slot, estate,
										   conflictRowidPtrDatum,
										   arbiterIndexes))
#endif
			{
				if (lockedSlot)
				{
					TM_Result	test;
					TM_FailureData tmfd;
					Datum		xminDatum;
					TransactionId xmin;
					bool		isnull;

					/* Determine lock mode to use */
					lockmode = ExecUpdateLockMode(estate, resultRelInfo);

					/*
					 * Lock tuple for update.  Don't follow updates when tuple
					 * cannot be locked without doing so.  A row locking
					 * conflict here means our previous conclusion that the
					 * tuple is conclusively committed is not true anymore.
					 */
					test = table_tuple_lock(rel, conflictRowid,
											estate->es_snapshot,
											lockedSlot, estate->es_output_cid,
											lockmode, LockWaitBlock, 0,
											&tmfd);
					switch (test)
					{
						case TM_Ok:
							/* success! */
							break;

						case TM_Invisible:

							/*
							 * This can occur when a just inserted tuple is
							 * updated again in the same command. E.g. because
							 * multiple rows with the same conflicting key
							 * values are inserted.
							 *
							 * This is somewhat similar to the ExecUpdate()
							 * TM_SelfModified case.  We do not want to
							 * proceed because it would lead to the same row
							 * being updated a second time in some unspecified
							 * order, and in contrast to plain UPDATEs there's
							 * no historical behavior to break.
							 *
							 * It is the user's responsibility to prevent this
							 * situation from occurring.  These problems are
							 * why the SQL standard similarly specifies that
							 * for SQL MERGE, an exception must be raised in
							 * the event of an attempt to update the same row
							 * twice.
							 */
							xminDatum = slot_getsysattr(lockedSlot,
														MinTransactionIdAttributeNumber,
														&isnull);
							Assert(!isnull);
							xmin = DatumGetTransactionId(xminDatum);

							if (TransactionIdIsCurrentTransactionId(xmin))
								ereport(ERROR,
										(errcode(ERRCODE_CARDINALITY_VIOLATION),
								/* translator: %s is a SQL command name */
										 errmsg("%s command cannot affect row a second time",
												"ON CONFLICT DO UPDATE"),
										 errhint("Ensure that no rows proposed for insertion within the same command have duplicate constrained values.")));

							/* This shouldn't happen */
							elog(ERROR, "attempted to lock invisible tuple");
							break;

						case TM_SelfModified:

							/*
							 * This state should never be reached. As a dirty
							 * snapshot is used to find conflicting tuples,
							 * speculative insertion wouldn't have seen this
							 * row to conflict with.
							 */
							elog(ERROR, "unexpected self-updated tuple");
							break;

						case TM_Updated:
							if (IsolationUsesXactSnapshot())
								ereport(ERROR,
										(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
										 errmsg("could not serialize access due to concurrent update")));

							/*
							 * As long as we don't support an UPDATE of INSERT
							 * ON CONFLICT for a partitioned table we
							 * shouldn't reach to a case where tuple to be
							 * lock is moved to another partition due to
							 * concurrent update of the partition key.
							 */
							Assert(!ItemPointerIndicatesMovedPartitions(&tmfd.ctid));

							/*
							 * Tell caller to try again from the very start.
							 *
							 * It does not make sense to use the usual
							 * EvalPlanQual() style loop here, as the new
							 * version of the row might not conflict anymore,
							 * or the conflicting tuple has actually been
							 * deleted.
							 */
							ExecClearTuple(lockedSlot);
							return NULL;

						case TM_Deleted:
							if (IsolationUsesXactSnapshot())
								ereport(ERROR,
										(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
										 errmsg("could not serialize access due to concurrent delete")));

							/* see TM_Updated case */
							Assert(!ItemPointerIndicatesMovedPartitions(&tmfd.ctid));
							ExecClearTuple(lockedSlot);
							return NULL;

						default:
							elog(ERROR, "unrecognized table_tuple_lock status: %u", test);
					}

					/* Success, the tuple is locked. */

					/*
					 * Verify that the tuple is visible to our MVCC snapshot
					 * if the current isolation level mandates that.
					 *
					 * It's not sufficient to rely on the check within
					 * ExecUpdate() as e.g. CONFLICT ... WHERE clause may
					 * prevent us from reaching that.
					 *
					 * This means we only ever continue when a new command in
					 * the current transaction could see the row, even though
					 * in READ COMMITTED mode the tuple will not be visible
					 * according to the current statement's snapshot.  This is
					 * in line with the way UPDATE deals with newer tuple
					 * versions.
					 */
					/* ExecCheckTupleVisible(estate, rel, lockedSlot); */
					return NULL;
				}
				else
				{
					/*
					 * ExecCheckTIDVisible(estate, rel, &conflictTid,
					 * tempSlot);
					 */
					return NULL;
				}
			}

			ExecInsertIndexTuples(resultRelInfo, slot, estate,
								  false, true, &specConflict,
								  arbiterIndexes, false);

			if (specConflict)
			{
				if (lockedSlot)
				{
					bytea	   *rowid;
					Pointer		p;
					OIndexDescr *primary = GET_PRIMARY(descr);

					ExecCopySlot(lockedSlot, slot);

					rowid = DatumGetByteaP(conflictRowid);
					p = (Pointer) rowid + MAXALIGN(VARHDRSZ);

					if (!primary->primaryIsCtid)
					{
						ORowIdAddendumNonCtid *add;
						OTuple		tuple;

						add = (ORowIdAddendumNonCtid *) p;
						p += MAXALIGN(sizeof(ORowIdAddendumNonCtid));

						if (primary->bridging)
							p += MAXALIGN(sizeof(ORowIdBridgeData));

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
										oxid, csn, &callbackInfo,
										UNIQUE_CHECK_YES);

			if (result != OBTreeModifyResultInserted)
			{
				success = false;
				failedIndexNumber = i;
			}
		}

		/* Successful insert case */
		if (success)
		{
			OIndexDescr *primary = GET_PRIMARY(descr);

			pgstat_count_heap_insert(rel, 1);

			/* all inserts are OK */
			tts_orioledb_insert_toast_values(slot, descr, oxid, csn);

			tup = tts_orioledb_form_tuple(slot, descr);

			if (primary->desc.storageType == BTreeStoragePersistence)
				o_wal_insert(&primary->desc, tup, rel->rd_rel->relreplident, descr->version);
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
	bool		was_saving;

	was_saving = o_start_saving_inval_messages();
	CheckCmdReplicaIdentity(rel, CMD_UPDATE);
	o_stop_saving_inval_messages(was_saving);

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

		was_saving = o_start_saving_inval_messages();
		/* not using simple reindex_relation here anymore, */
		/* because we hold a lock on relation already */
		indexIds = RelationGetIndexList(rel);

		oldSlot = arg->scanSlot;
		newSlot = &arg->newSlot->base;
		Assert(oldSlot->tts_tupleDescriptor->natts == newSlot->tts_tupleDescriptor->natts);
		for (attnum = 0; attnum < oldSlot->tts_nvalid; attnum++)
		{
			OTupleAttrCompact *attr = OTupleDescAttrFast(oldSlot->tts_tupleDescriptor,
														 attnum);

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
		o_stop_saving_inval_messages(was_saving);
	}

	tts_orioledb_toast(slot, descr);
	tts_orioledb_fill_key_bound(slot, GET_PRIMARY(descr), &newPkey);
	if (touched_indices)
		o_apply_new_bridge_index_ctid(descr, rel, slot, csn, true);

	newTup = tts_orioledb_form_tuple(slot, descr);
	o_btree_check_size_of_tuple(o_tuple_size(newTup, &primary->leafSpec),
								RelationGetRelationName(rel),
								false);

	if (is_keys_eq(GET_PRIMARY(descr), oldPkey, &newPkey))
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
				o_wal_update(&primary->desc, final_tup, ((OTableSlot *) oldSlot)->tuple, rel->rd_rel->relreplident, descr->version);
			}
		}
		else if (mres.action == BTreeOperationDelete)
		{
			if (descr->bridge)
			{
				delete_old_bridge_index_ctid(descr, rel, &((OTableSlot *) oldSlot)->bridge_ctid, csn);
				if (!touched_indices)
					o_apply_new_bridge_index_ctid(descr, rel, slot, csn, false);
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

				o_wal_reinsert(&primary->desc, ((OTableSlot *) oldSlot)->tuple, final_tup, rel->rd_rel->relreplident, descr->version);
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
	bool		was_saving;

	was_saving = o_start_saving_inval_messages();
	CheckCmdReplicaIdentity(rel, CMD_DELETE);
	o_stop_saving_inval_messages(was_saving);

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
				o_wal_delete(&primary->desc, primary_tuple, rel->rd_rel->relreplident, descr->version);
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

		typid = TupleDescAttr(idx->nonLeafTupdesc, i)->atttypid;

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

	if (is_keys_eq(id, &old_key, &new_key) && (old_valid == new_valid))
		return res;

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
		.arg = arg,
		.postUndoRecorded = set_pending_sk_marker_from_modify_arg
	};

	memset(&result, 0, sizeof(result));
	result.success = true;
	result.oldTuple = NULL;

	newTup = tts_orioledb_form_tuple(newSlot, descr);

	modify_result = o_btree_modify(&GET_PRIMARY(descr)->desc, BTreeOperationUpdate,
								   newTup, BTreeKeyLeafTuple,
								   (Pointer) oldPkey, BTreeKeyBound,
								   oxid, csn, RowLockNoKeyUpdate,
								   hint, &callbackInfo);
	fire_sk_modify_pending_stopevent(descr);

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
		((OTableSlot *) newSlot)->version = o_tuple_get_version(((OTableSlot *) newSlot)->tuple);
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
		.arg = newSlot,
		.postUndoRecorded = set_pending_sk_marker_from_slot
	};

	memset(&result, 0, sizeof(result));
	result.success = true;
	result.oldTuple = NULL;

	newTup = tts_orioledb_form_tuple(newSlot, descr);

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
	fire_sk_modify_pending_stopevent(descr);
	((OTableSlot *) newSlot)->version = o_tuple_get_version(((OTableSlot *) newSlot)->tuple);

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
		.arg = arg,
		.postUndoRecorded = set_pending_sk_marker_from_modify_arg
	};

	memset(&result, 0, sizeof(result));
	result.oldTuple = NULL;

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
	fire_sk_modify_pending_stopevent(descr);

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

/*
 * Check if two keys are binary equal.
 */
static inline bool
is_keys_eq(OIndexDescr *id, OBTreeKeyBound *k1, OBTreeKeyBound *k2)
{
	int			i,
				n;

	if (k1->nkeys != k2->nkeys)
		return false;

	if (id->desc.type == oIndexPrimary)
		n = id->nUniqueFields;
	else
		n = id->nonLeafTupdesc->natts;

	Assert(n <= k1->nkeys && n <= k2->nkeys);

	for (i = 0; i < n; i++)
	{
		OTupleAttrCompact *attr = OTupleDescAttrFast(id->nonLeafTupdesc, i);

		if (k1->keys[i].flags != k2->keys[i].flags)
			return false;
		if (k1->keys[i].flags & O_VALUE_BOUND_NO_VALUE)
			continue;

		if (!datum_image_eq(k1->keys[i].value, k2->keys[i].value,
							attr->attbyval, attr->attlen))
			return false;
	}
	return true;
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
							 TupleDescAttr(id->nonLeafTupdesc, i)->attname.data);
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
	OIndexKey  *trees;
	OTable	   *o_table;
	int			treesNum;
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

	trees = o_table_make_index_keys(o_table, &treesNum);

	for (i = 0; i < treesNum; i++)
	{
		o_tables_rel_lock_extended(&trees[i].oids, AccessExclusiveLock, false);
		o_tables_rel_lock_extended(&trees[i].oids, AccessExclusiveLock, true);
		cleanup_btree(trees[i], true, !is_temp);
		o_invalidate_oids(trees[i].oids);
/*		if (is_recovery_process())
			o_invalidate_descrs(trees[i].datoid, trees[i].reloid,
								trees[i].relnode);*/
		if (ORelOidsIsEqual(oids, trees[i].oids))
			invalidatedTable = true;
		o_tables_rel_unlock_extended(&trees[i].oids, AccessExclusiveLock, false);
		o_tables_rel_unlock_extended(&trees[i].oids, AccessExclusiveLock, true);
	}

	if (!invalidatedTable)
	{
		OIndexKey	key = {.oids = oids,.tablespace = o_table->tablespace};

		cleanup_btree(key, true, !is_temp);
		o_invalidate_oids(oids);
/*		if (is_recovery_process())
			o_invalidate_descrs(oids.datoid, oids.reloid, oids.relnode);*/
	}

	o_tables_rel_unlock(&oids, AccessExclusiveLock);

	pfree(trees);
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
