/*-------------------------------------------------------------------------
 *
 * handler.c
 *		Implementation of table access method handler
 *
 * Copyright (c) 2021-2022, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/tableam/handler.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>

#include "orioledb.h"

#include "btree/btree.h"
#include "btree/iterator.h"
#include "btree/scan.h"
#include "btree/undo.h"
#include "catalog/indices.h"
#include "catalog/o_opclass.h"
#include "catalog/o_tables.h"
#include "catalog/o_type_cache.h"
#include "tableam/descr.h"
#include "tableam/handler.h"
#include "tableam/operations.h"
#include "tableam/tree.h"
#include "transam/oxid.h"
#include "tuple/slot.h"
#include "utils/stopevent.h"

#include "access/heapam.h"
#include "access/multixact.h"
#include "access/tableam.h"
#include "catalog/index.h"
#include "catalog/pg_am_d.h"
#include "catalog/storage.h"
#include "commands/progress.h"
#include "commands/vacuum.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "optimizer/optimizer.h"
#include "optimizer/plancat.h"
#include "parser/parsetree.h"
#include "storage/bufmgr.h"
#if PG_VERSION_NUM >= 140000
#include "utils/backend_progress.h"
#else
#include "pgstat.h"
#endif
#include "utils/lsyscache.h"
#include "utils/sampling.h"

typedef struct OScanDescData
{
	TableScanDescData rs_base;	/* AM independent part of the descriptor */
	BTreeSeqScan *scan;
	CommitSeqNo csn;
	ItemPointerData iptr;
} OScanDescData;
typedef OScanDescData *OScanDesc;

typedef struct
{
	Oid			conflictIxOid;
} OFDWState;

/*
 * Operation with indices. It does not update TOAST BTree. Implementations
 * are in tableam_handler.c.
 */
static void get_keys_from_rowid(OIndexDescr *id, Datum pkDatum, OBTreeKeyBound *key,
								BTreeLocationHint *hint, CommitSeqNo *csn,
								uint32 *version);


/* ------------------------------------------------------------------------
 * Slot related callbacks for heap AM
 * ------------------------------------------------------------------------
 */

static const TupleTableSlotOps *
orioledb_slot_callbacks(Relation relation)
{
	/* TODO: Create own TupleTableSlotOps */
	return &TTSOpsOrioleDB;
}

/* ------------------------------------------------------------------------
 * Index Scan Callbacks for heap AM
 * ------------------------------------------------------------------------
 */

/*
 * Returns NULL to prevent index scan from inside of standard_planner
 * for greater and lower where clauses.
 */
static IndexFetchTableData *
orioledb_index_fetch_begin(Relation rel)
{
	return NULL;
}

static void
orioledb_index_fetch_reset(IndexFetchTableData *scan)
{
	elog(ERROR, "Not implemented: %s", PG_FUNCNAME_MACRO);
}

static void
orioledb_index_fetch_end(IndexFetchTableData *scan)
{
	elog(ERROR, "Not implemented: %s", PG_FUNCNAME_MACRO);
}

static bool
orioledb_index_fetch_tuple(struct IndexFetchTableData *scan,
						   ItemPointer tid,
						   Snapshot snapshot,
						   TupleTableSlot *slot,
						   bool *call_again, bool *all_dead)
{
	elog(ERROR, "Not implemented: %s", PG_FUNCNAME_MACRO);
	return false;
}


/* ------------------------------------------------------------------------
 * Callbacks for non-modifying operations on individual tuples for heap AM
 * ------------------------------------------------------------------------
 */

static bool
orioledb_tableam_fetch_row_version(Relation relation,
								   ItemPointer tid,
								   Snapshot snapshot,
								   TupleTableSlot *slot)
{
	elog(ERROR, "Not implemented: %s", PG_FUNCNAME_MACRO);
	return true;
}

static TupleFetchCallbackResult
fetch_row_version_callback(OTuple tuple, OXid tupOxid, CommitSeqNo csn,
						   void *arg, TupleFetchCallbackCheckType check_type)
{
	uint32		version = *((uint32 *) arg);

	if (check_type != OTupleFetchCallbackVersionCheck)
		return OTupleFetchNext;

	if (!(COMMITSEQNO_IS_INPROGRESS(csn) && tupOxid == get_current_oxid_if_any()))
		return OTupleFetchNext;

	if (o_tuple_get_version(tuple) <= version)
		return OTupleFetchMatch;
	else
		return OTupleFetchNext;
}


/*
 * Fetches last committed row version for given tupleid.
 */
static bool
orioledb_fetch_row_version(Relation relation,
						   Datum tupleid,
						   Snapshot snapshot,
						   TupleTableSlot *slot)
{
	OBTreeKeyBound pkey;
	OTableDescr *descr;
	OTuple		tuple;
	BTreeLocationHint hint;
	CommitSeqNo tupleCsn;
	CommitSeqNo csn;
	uint32		version;
	bool		deleted;

	descr = relation_get_descr(relation);

	Assert(descr != NULL);
	if (GET_PRIMARY(descr)->primaryIsCtid)
		o_btree_load_shmem(&GET_PRIMARY(descr)->desc);

	get_keys_from_rowid(GET_PRIMARY(descr), tupleid, &pkey, &hint,
						&csn, &version);

	tuple = o_btree_find_tuple_by_key_cb(&GET_PRIMARY(descr)->desc,
										 (Pointer) &pkey,
										 BTreeKeyBound,
										 csn, &tupleCsn,
										 slot->tts_mcxt,
										 &hint,
										 &deleted,
										 fetch_row_version_callback,
										 &version);

#if PG_VERSION_NUM >= 150000
	if (is_merge && deleted && COMMITSEQNO_IS_INPROGRESS(tupleCsn))
		return true;
#endif

	if (O_TUPLE_IS_NULL(tuple))
		return false;

	tts_orioledb_store_tuple(slot, tuple, descr, tupleCsn,
							 PrimaryIndexNumber, true, &hint);
	slot->tts_tableOid = RelationGetRelid(relation);

	return true;
}

/*
 * Refetches row version in given slot.
 */
static bool
orioledb_refetch_row_version(Relation rel, TupleTableSlot *slot)
{
	OBTreeKeyBound pkey;
	OTableDescr *descr;
	OTuple		tuple;
	CommitSeqNo tupleCsn;
	BTreeLocationHint hint = {OInvalidInMemoryBlkno, 0};

	descr = relation_get_descr(rel);

	Assert(descr != NULL);
	if (GET_PRIMARY(descr)->primaryIsCtid)
		o_btree_load_shmem(&GET_PRIMARY(descr)->desc);

	tts_orioledb_fill_key_bound(slot, GET_PRIMARY(descr), &pkey);

	tuple = o_btree_find_tuple_by_key(&GET_PRIMARY(descr)->desc,
									  (Pointer) &pkey,
									  BTreeKeyBound,
									  COMMITSEQNO_INPROGRESS, &tupleCsn,
									  slot->tts_mcxt,
									  &hint);

	if (O_TUPLE_IS_NULL(tuple))
		return false;

	tts_orioledb_store_tuple(slot, tuple, descr, tupleCsn,
							 PrimaryIndexNumber, true, &hint);
	slot->tts_tableOid = RelationGetRelid(rel);

	return true;
}

static bool
orioledb_tuple_tid_valid(TableScanDesc scan, ItemPointer tid)
{
	elog(ERROR, "Not implemented: %s", PG_FUNCNAME_MACRO);
	return false;
}

static bool
orioledb_tuple_satisfies_snapshot(Relation rel, TupleTableSlot *slot,
								  Snapshot snapshot)
{
	return true;
}


/* ----------------------------------------------------------------------------
 *  Functions for manipulations of physical tuples for heap AM.
 * ----------------------------------------------------------------------------
 */

static RowRefType
orioledb_get_row_ref_type(Relation rel)
{
	OTableDescr *descr;

	descr = relation_get_descr(rel);
	if (!descr)
	{
		/*
		 * It happens during relation creation.  Should be safe to assume
		 * we've TID identifiers at this point.
		 */
		return ROW_REF_TID;
	}

	/*
	 * Always use rowid identifieds.  If even we use ctid as primary key, we
	 * still prepend it with page location hint.
	 */
	return ROW_REF_ROWID;
}

static void
orioledb_tuple_insert(Relation relation, TupleTableSlot *slot,
					  EState *estate, CommandId cid,
					  int options, BulkInsertState bistate)
{
	OTableDescr *descr;
	CommitSeqNo csn;
	OXid		oxid;

	descr = relation_get_descr(relation);
	fill_current_oxid_csn(&oxid, &csn);
	o_tbl_insert(descr, relation, estate, slot, oxid, csn);
}

static void
orioledb_tableam_tuple_insert(Relation relation, TupleTableSlot *slot,
							  CommandId cid, int options,
							  BulkInsertState bistate)
{
	elog(ERROR, "Not implemented: %s", PG_FUNCNAME_MACRO);
}

static void
orioledb_tuple_insert_speculative(Relation relation, TupleTableSlot *slot,
								  CommandId cid, int options,
								  BulkInsertState bistate, uint32 specToken)
{
	elog(ERROR, "Not implemented: %s", PG_FUNCNAME_MACRO);
}

static void
orioledb_tuple_complete_speculative(Relation relation, TupleTableSlot *slot,
									uint32 specToken, bool succeeded)
{
	elog(ERROR, "Not implemented: %s", PG_FUNCNAME_MACRO);
}

static TM_Result
orioledb_tableam_tuple_delete(Relation relation, ItemPointer tid, CommandId cid,
							  Snapshot snapshot, Snapshot crosscheck, bool wait,
							  TM_FailureData *tmfd, bool changingPart)
{
	elog(ERROR, "Not implemented: %s", PG_FUNCNAME_MACRO);
	return TM_Ok;
}

static OIndexNumber
get_ix_num_by_oid(OTableDescr *descr, Oid ixOid)
{
	OIndexNumber ixNum;

	if (!OidIsValid(ixOid))
		return InvalidIndexNumber;

	for (ixNum = 0; ixNum < descr->nIndices; ixNum++)
	{
		if (descr->indices[ixNum]->oids.reloid == ixOid)
			return ixNum;
	}

	return InvalidIndexNumber;
}

static TupleTableSlot *
orioledb_tuple_insert_on_conflict(ModifyTableState *mstate,
								  EState *estate,
								  ResultRelInfo *rinfo,
								  TupleTableSlot *slot)
{
	OTableDescr *descr;
	OTuple		tup;
	Relation	rel = rinfo->ri_RelationDesc;
	OnConflictAction on_conflict = ((ModifyTable *) mstate->ps.plan)->onConflictAction;
	OFDWState  *state = (OFDWState *) rinfo->ri_FdwState;
	OIndexDescr *id;

	o_check_constraints(rinfo, slot, estate);

	descr = relation_get_descr(rel);
	Assert(descr);
	id = GET_PRIMARY(descr);

	if (id->primaryIsCtid)
	{
		o_btree_load_shmem(&id->desc);
		slot->tts_tid = btree_ctid_get_and_inc(&id->desc);
	}

	tts_orioledb_toast(slot, descr);

	tup = tts_orioledb_form_tuple(slot, descr);
	o_btree_check_size_of_tuple(o_tuple_size(tup, &id->leafSpec),
								RelationGetRelationName(rel),
								false);

	slot = o_tbl_insert_on_conflict(mstate, estate, rinfo,
									descr, slot,
									on_conflict,
									get_ix_num_by_oid(descr, state->conflictIxOid));

	return slot;
}

static TM_Result
orioledb_tuple_delete(ModifyTableState *mstate,
					  ResultRelInfo *rinfo,
					  EState *estate,
					  Datum tupleid,
					  TupleTableSlot *returningSlot,
					  CommandId cid,
					  Snapshot snapshot, Snapshot crosscheck, bool wait,
					  TM_FailureData *tmfd, bool changingPart)
{
	OModifyCallbackArg marg;
	OTableModifyResult mres;
	OBTreeKeyBound pkey;
	OTableDescr *descr;
	OXid		oxid;
	BTreeLocationHint hint;

	descr = relation_get_descr(rinfo->ri_RelationDesc);

	Assert(descr != NULL);
	if (GET_PRIMARY(descr)->primaryIsCtid)
		o_btree_load_shmem(&GET_PRIMARY(descr)->desc);

	oxid = get_current_oxid();

	marg.descr = descr;
	marg.oxid = oxid;
	marg.csn = snapshot->snapshotcsn;
	marg.rinfo = rinfo;
	marg.epqstate = &mstate->mt_epqstate;
	marg.scanSlot = returningSlot ? returningSlot : descr->oldTuple;
	marg.rowLockMode = RowLockUpdate;

	get_keys_from_rowid(GET_PRIMARY(descr), tupleid, &pkey, &hint, NULL, NULL);

	mres = o_tbl_delete(descr, estate, &pkey, oxid, snapshot->snapshotcsn,
						&hint, &marg);

	if (mres.success && mres.action == BTreeOperationLock)
	{
		TupleTableSlot *epqslot;

		epqslot = EvalPlanQualNext(marg.epqstate);
		if (TupIsNull(epqslot))
			/* Tuple not passing quals anymore, exiting... */
			return TM_Deleted;

		tts_orioledb_fill_key_bound(mres.oldTuple, GET_PRIMARY(descr), &pkey);
		mres = o_tbl_delete(descr, estate, &pkey, oxid, marg.csn,
							&hint, &marg);
		Assert(mres.action != BTreeOperationLock);
		/* ExecClearTuple(mres.oldTuple); */
	}

	o_check_tbl_delete_mres(mres, descr, rinfo->ri_RelationDesc);

	if (mres.self_modified)
		tmfd->xmax = GetCurrentTransactionId();
	else
		tmfd->xmax = InvalidTransactionId;

	if (mres.success)
	{
		Assert(mres.oldTuple != NULL);
		if (returningSlot && mres.oldTuple != returningSlot)
			ExecCopySlot(returningSlot, mres.oldTuple);

		return mres.self_modified ? TM_SelfModified : TM_Ok;
	}

	return mres.self_modified ? TM_SelfModified : TM_Deleted;
}

static TM_Result
orioledb_tableam_tuple_update(Relation relation, ItemPointer otid, TupleTableSlot *slot,
							  CommandId cid, Snapshot snapshot, Snapshot crosscheck,
							  bool wait, TM_FailureData *tmfd,
							  LockTupleMode *lockmode, bool *update_indexes)
{
	elog(ERROR, "Not implemented: %s", PG_FUNCNAME_MACRO);
	return TM_Ok;
}

static TM_Result
orioledb_tuple_update(ModifyTableState *mstate, ResultRelInfo *rinfo,
					  EState *estate,
					  Datum tupleid, TupleTableSlot *slot,
					  CommandId cid, Snapshot snapshot, Snapshot crosscheck,
					  bool wait, TM_FailureData *tmfd,
					  LockTupleMode *lockmode, bool *update_indexes)
{
	OTableModifyResult mres;
	OModifyCallbackArg marg;
	Relation	rel;
	Bitmapset  *updatedAttrs;
	Bitmapset  *keyAttrs;
	RangeTblEntry *rte;
	OBTreeKeyBound old_pkey;
	OTableDescr *descr;
	OXid		oxid;
	BTreeLocationHint hint;

	rel = rinfo->ri_RelationDesc;

	descr = relation_get_descr(rinfo->ri_RelationDesc);

	Assert(descr != NULL);
	if (GET_PRIMARY(descr)->primaryIsCtid)
		o_btree_load_shmem(&GET_PRIMARY(descr)->desc);

	o_check_constraints(rinfo, slot, mstate->ps.state);

	*update_indexes = false;
	oxid = get_current_oxid();

	get_keys_from_rowid(GET_PRIMARY(descr), tupleid, &old_pkey, &hint,
						NULL, NULL);

	marg.descr = descr;
	marg.oxid = oxid;
	marg.csn = snapshot->snapshotcsn;
	marg.rinfo = rinfo;
	marg.epqstate = &mstate->mt_epqstate;
	marg.scanSlot = descr->oldTuple;
	marg.newSlot = (OTableSlot *) slot;

	/*
	 * Get appropriate row lock mode.
	 *
	 * We don't have current version of tuple at hands and it appears to be
	 * difficult for postgres executor to bring it to us.  Fetching previous
	 * version of tuple to get row lock mode would require additional
	 * roundtrip. So, instead of getting row lock mode from actually updated
	 * columns, we get it from SET clause of UPDATE command.  In the majority
	 * of cases result should be the same.
	 */
	keyAttrs = RelationGetIndexAttrBitmap(rinfo->ri_RelationDesc,
										  INDEX_ATTR_BITMAP_KEY);
	rte = exec_rt_fetch(rinfo->ri_RangeTableIndex, mstate->ps.state);
	updatedAttrs = rte->updatedCols;
	if (!bms_overlap(keyAttrs, updatedAttrs))
		marg.rowLockMode = RowLockNoKeyUpdate;
	else
		marg.rowLockMode = RowLockUpdate;

	mres = o_tbl_update(descr, slot, estate, &old_pkey, rel,
						oxid, snapshot->snapshotcsn, &hint, &marg);

	if (mres.success && mres.action == BTreeOperationLock)
	{
		TupleTableSlot *epqslot;
		bool		partition_constraint_failed;

		epqslot = EvalPlanQualNext(marg.epqstate);
		if (TupIsNull(epqslot))
			/* Tuple not passing quals anymore, exiting... */
			return TM_Deleted;

#if PG_VERSION_NUM >= 140000
		(void) ExecGetUpdateNewTuple(rinfo,
									 epqslot,
									 mres.oldTuple);
#else
		slot = ExecFilterJunk(mstate->ps.state->es_result_relation_info->ri_junkFilter,
							  epqslot);
#endif

		partition_constraint_failed =
			rel->rd_rel->relispartition &&
			!ExecPartitionCheck(rinfo, slot, estate, false);
		if (!partition_constraint_failed &&
			rinfo->ri_WithCheckOptions != NIL)
		{
			/*
			 * ExecWithCheckOptions() will skip any WCOs which are not of the
			 * kind we are looking for at this point.
			 */
			ExecWithCheckOptions(WCO_RLS_UPDATE_CHECK,
								 rinfo, slot, estate);
		}

		if (partition_constraint_failed)
			return TM_Updated;

		if (rel->rd_att->constr)
			ExecConstraints(rinfo, slot, estate);

		tts_orioledb_fill_key_bound(mres.oldTuple, GET_PRIMARY(descr), &old_pkey);
		mres = o_tbl_update(descr, slot, estate, &old_pkey, rel,
							oxid, marg.csn, &hint, &marg);
		Assert(mres.action != BTreeOperationLock);
		/* ExecClearTuple(mres.oldTuple); */
	}

	o_check_tbl_update_mres(mres, descr, rel, slot);

	Assert(mres.success);

	if (mres.self_modified)
		tmfd->xmax = GetCurrentTransactionId();
	else
		tmfd->xmax = InvalidTransactionId;

	if (mres.oldTuple)
		return mres.self_modified ? TM_SelfModified : TM_Ok;
	return TM_Deleted;
}

static TM_Result
orioledb_tableam_tuple_lock(Relation relation, ItemPointer tid, Snapshot snapshot,
							TupleTableSlot *slot, CommandId cid, LockTupleMode mode,
							LockWaitPolicy wait_policy, uint8 flags,
							TM_FailureData *tmfd)
{
	elog(ERROR, "Not implemented: %s", PG_FUNCNAME_MACRO);
	return TM_Ok;
}

static TM_Result
orioledb_tuple_lock(Relation rel, Datum tupleid, Snapshot snapshot,
					TupleTableSlot *slot, CommandId cid, LockTupleMode mode,
					LockWaitPolicy wait_policy, uint8 flags,
					TM_FailureData *tmfd)
{
	OLockCallbackArg larg;
	OBTreeModifyResult res;
	OBTreeKeyBound pkey;
	OTableDescr *descr;
	OXid		oxid;
	BTreeLocationHint hint;

	descr = relation_get_descr(rel);
	Assert(descr != NULL);

	oxid = get_current_oxid();

	larg.rel = rel;
	larg.descr = descr;
	larg.oxid = oxid;
	larg.csn = snapshot->snapshotcsn;
	larg.epqstate = NULL;
	larg.scanSlot = slot;
	larg.waitPolicy = wait_policy;
	larg.wouldBlock = false;
	larg.modified = false;

	get_keys_from_rowid(GET_PRIMARY(descr), tupleid, &pkey, &hint, NULL, NULL);

	res = o_tbl_lock(descr, &pkey, mode, oxid, &larg, &hint);

	if (larg.modified)
		tmfd->traversed = true;
	else
		tmfd->traversed = false;

	if (larg.wouldBlock)
		return TM_WouldBlock;

	if (res == OBTreeModifyResultNotFound)
		return TM_Deleted;

	Assert(res == OBTreeModifyResultLocked);

	return TM_Ok;
}

static void
orioledb_finish_bulk_insert(Relation relation, int options)
{
	/* Do nothing here */
}


/* ------------------------------------------------------------------------
 * DDL related callbacks for heap AM.
 * ------------------------------------------------------------------------
 */

static void
orioledb_relation_set_new_filenode(Relation rel,
								   const RelFileNode *newrnode,
								   char persistence,
								   TransactionId *freezeXid,
								   MultiXactId *minmulti)
{
	SMgrRelation srel;

	/* TRUNCATE case */
	if (rel->rd_rel->oid != 0 &&
		rel->rd_rel->relkind != RELKIND_TOASTVALUE &&
		!is_in_indexes_rebuild())
	{
		OTable	   *o_table;
		CommitSeqNo csn;
		OXid		oxid;
		int			oldTreeOidsNum,
					newTreeOidsNum;
		ORelOids	oldOids,
				   *oldTreeOids,
					newOids,
				   *newTreeOids;

		oldOids.datoid = MyDatabaseId;
		oldOids.reloid = rel->rd_rel->oid;
		oldOids.relnode = rel->rd_node.relNode;

		fill_current_oxid_csn(&oxid, &csn);

		o_table = o_tables_get(oldOids);
		Assert(o_table != NULL);
		oldTreeOids = o_table_make_index_oids(o_table, &oldTreeOidsNum);

		o_table_fill_oids(o_table, rel, newrnode);

		newOids = o_table->oids;
		newTreeOids = o_table_make_index_oids(o_table, &newTreeOidsNum);

		LWLockAcquire(&checkpoint_state->oTablesAddLock, LW_SHARED);
		o_tables_drop_by_oids(oldOids, oxid, csn);
		o_tables_add(o_table, oxid, csn);
		LWLockRelease(&checkpoint_state->oTablesAddLock);
		o_table_free(o_table);

		orioledb_free_rd_amcache(rel);

		Assert(o_fetch_table_descr(newOids) != NULL);
		add_undo_truncate_relnode(oldOids, oldTreeOids, oldTreeOidsNum,
								  newOids, newTreeOids, newTreeOidsNum);
		pfree(oldTreeOids);
		pfree(newTreeOids);
	}

	*freezeXid = InvalidTransactionId;
	*minmulti = InvalidMultiXactId;

#if PG_VERSION_NUM >= 150000
	srel = RelationCreateStorage(*newrnode, persistence, false);
#else
	srel = RelationCreateStorage(*newrnode, persistence);
#endif
	smgrclose(srel);
}

static void
orioledb_relation_nontransactional_truncate(Relation rel)
{
	ORelOids	oids = {MyDatabaseId, rel->rd_rel->oid,
	rel->rd_node.relNode},
			   *treeOids;
	OTable	   *o_table;
	int			treeOidsNum;
	int			i;
	bool		invalidatedTable = false;

	if (rel->rd_rel->oid == 0 || rel->rd_rel->relkind == RELKIND_TOASTVALUE)
		return;

	o_tables_rel_lock(&oids, AccessExclusiveLock);

	o_table = o_tables_get(oids);
	Assert(o_table != NULL);

	treeOids = o_table_make_index_oids(o_table, &treeOidsNum);

	for (i = 0; i < treeOidsNum; i++)
	{
		o_tables_rel_lock_extended(&treeOids[i], AccessExclusiveLock, false);
		o_tables_rel_lock_extended(&treeOids[i], AccessExclusiveLock, true);
		cleanup_btree(treeOids[i].datoid, treeOids[i].relnode);
		o_invalidate_oids(treeOids[i]);
		if (ORelOidsIsEqual(oids, treeOids[i]))
			invalidatedTable = true;
		o_tables_rel_unlock_extended(&treeOids[i], AccessExclusiveLock, false);
		o_tables_rel_unlock_extended(&treeOids[i], AccessExclusiveLock, true);
	}

	if (!invalidatedTable)
		o_invalidate_oids(oids);

	o_tables_rel_unlock(&oids, AccessExclusiveLock);

	pfree(treeOids);
}

static void
orioledb_relation_copy_data(Relation rel, const RelFileNode *newrnode)
{
	elog(ERROR, "Not implemented: %s", PG_FUNCNAME_MACRO);
}

static void
orioledb_relation_copy_for_cluster(Relation OldHeap, Relation NewHeap,
								   Relation OldIndex, bool use_sort,
								   TransactionId OldestXmin,
								   TransactionId *xid_cutoff,
								   MultiXactId *multi_cutoff,
								   double *num_tuples,
								   double *tups_vacuumed,
								   double *tups_recently_dead)
{
	elog(ERROR, "Not implemented: %s", PG_FUNCNAME_MACRO);
}

static bool
orioledb_scan_analyze_next_block(TableScanDesc scan, BlockNumber blockno,
								 BufferAccessStrategy bstrategy)
{
	OScanDesc	oscan = (OScanDesc) scan;

	ItemPointerSetBlockNumber(&oscan->iptr, blockno);
	ItemPointerSetOffsetNumber(&oscan->iptr, 1);

	return true;
}

#define NUM_TUPLES_PER_BLOCK	128

static bool
orioledb_scan_analyze_next_tuple(TableScanDesc scan, TransactionId OldestXmin,
								 double *liverows, double *deadrows,
								 TupleTableSlot *slot)
{
	OScanDesc	oscan = (OScanDesc) scan;
	OTableDescr *descr;
	BTreeLocationHint hint;
	OTuple		tuple;
	bool		end;

	descr = relation_get_descr(scan->rs_rd);

	while (true)
	{
		tuple = btree_seq_scan_getnext_raw(oscan->scan, slot->tts_mcxt, &end, &hint);

		if (end || ItemPointerGetOffsetNumber(&oscan->iptr) > NUM_TUPLES_PER_BLOCK)
			return false;

		if (!O_TUPLE_IS_NULL(tuple))
		{
			tts_orioledb_store_tuple(slot, tuple, descr, oscan->csn,
									 PrimaryIndexNumber, false, &hint);

			*liverows += 1;
			ItemPointerSetBlockNumber(&slot->tts_tid, ItemPointerGetBlockNumber(&oscan->iptr));
			ItemPointerSetOffsetNumber(&slot->tts_tid, ItemPointerGetOffsetNumber(&oscan->iptr));
			ItemPointerSetOffsetNumber(&oscan->iptr, ItemPointerGetOffsetNumber(&oscan->iptr) + 1);
			return true;
		}
		else
		{
			*deadrows += 1;
		}
	}
}

static double
orioledb_index_build_range_scan(Relation heapRelation,
								Relation indexRelation,
								IndexInfo *indexInfo,
								bool allow_sync,
								bool anyvisible,
								bool progress,
								BlockNumber start_blockno,
								BlockNumber numblocks,
								IndexBuildCallback callback,
								void *callback_state,
								TableScanDesc scan)
{
	/*
	 * used for index creation
	 */
	return 0.0;
}

static void
orioledb_index_validate_scan(Relation heapRelation,
							 Relation indexRelation,
							 IndexInfo *indexInfo,
							 Snapshot snapshot,
							 ValidateIndexState *state)
{
	elog(ERROR, "Not implemented: %s", PG_FUNCNAME_MACRO);
}


/* ------------------------------------------------------------------------
 * Miscellaneous callbacks for the heap AM
 * ------------------------------------------------------------------------
 */

static uint64
orioledb_relation_size(Relation rel, ForkNumber forkNumber)
{
	OTableDescr *descr;

	descr = relation_get_descr(rel);

	if (descr && tbl_data_exists(&GET_PRIMARY(descr)->oids))
	{
		o_btree_load_shmem(&GET_PRIMARY(descr)->desc);
		return (uint64) TREE_NUM_LEAF_PAGES(&GET_PRIMARY(descr)->desc) *
			ORIOLEDB_BLCKSZ;
	}
	else
	{
		return 0;
	}
}

static bool
orioledb_relation_needs_toast_table(Relation rel)
{
	return true;
}

static Oid
orioledb_relation_toast_am(Relation rel)
{
	return HEAP_TABLE_AM_OID;
}


/* ------------------------------------------------------------------------
 * Planner related callbacks for the heap AM
 * ------------------------------------------------------------------------
 */

static void
orioledb_estimate_rel_size(Relation rel, int32 *attr_widths,
						   BlockNumber *pages, double *tuples,
						   double *allvisfrac)
{
	BlockNumber curpages;
	BlockNumber relpages;
	double		reltuples;
	BlockNumber relallvisible;
	double		density;

	/* it has storage, ok to call the smgr */
	curpages = RelationGetNumberOfBlocks(rel);

	/* coerce values in pg_class to more desirable types */
	relpages = (BlockNumber) rel->rd_rel->relpages;
	reltuples = (double) rel->rd_rel->reltuples;
	relallvisible = (BlockNumber) rel->rd_rel->relallvisible;

	/*
	 * HACK: if the relation has never yet been vacuumed, use a minimum size
	 * estimate of 10 pages.  The idea here is to avoid assuming a
	 * newly-created table is really small, even if it currently is, because
	 * that may not be true once some data gets loaded into it.  Once a vacuum
	 * or analyze cycle has been done on it, it's more reasonable to believe
	 * the size is somewhat stable.
	 *
	 * (Note that this is only an issue if the plan gets cached and used again
	 * after the table has been filled.  What we're trying to avoid is using a
	 * nestloop-type plan on a table that has grown substantially since the
	 * plan was made.  Normally, autovacuum/autoanalyze will occur once enough
	 * inserts have happened and cause cached-plan invalidation; but that
	 * doesn't happen instantaneously, and it won't happen at all for cases
	 * such as temporary tables.)
	 *
	 * We approximate "never vacuumed" by "has relpages = 0", which means this
	 * will also fire on genuinely empty relations.  Not great, but
	 * fortunately that's a seldom-seen case in the real world, and it
	 * shouldn't degrade the quality of the plan too much anyway to err in
	 * this direction.
	 *
	 * If the table has inheritance children, we don't apply this heuristic.
	 * Totally empty parent tables are quite common, so we should be willing
	 * to believe that they are empty.
	 */
	if (curpages < 10 &&
		relpages == 0 &&
		!rel->rd_rel->relhassubclass)
		curpages = 10;

	/* report estimated # pages */
	*pages = curpages;
	/* quick exit if rel is clearly empty */
	if (curpages == 0)
	{
		*tuples = 0;
		*allvisfrac = 0;
		return;
	}

	/* estimate number of tuples from previous tuple density */
	if (relpages > 0)
	{
		density = reltuples / (double) relpages;
		*tuples = rint(density * (double) curpages);
	}
	else
	{
		/*
		 * When we have no data because the relation was truncated, estimate
		 * tuple width from attribute datatypes.  We assume here that the
		 * pages are completely full, which is OK for tables (since they've
		 * presumably not been VACUUMed yet) but is probably an overestimate
		 * for indexes.  Fortunately get_relation_info() can clamp the
		 * overestimate to the parent table's size.
		 *
		 * Note: this code intentionally disregards alignment considerations,
		 * because (a) that would be gilding the lily considering how crude
		 * the estimate is, and (b) it creates platform dependencies in the
		 * default plans which are kind of a headache for regression testing.
		 */
		int32		tuple_width;

		tuple_width = get_rel_data_width(rel, attr_widths);
		tuple_width += MAXALIGN(SizeOfOTupleHeader);
		/* note: integer division is intentional here */
		density = ((double) (ORIOLEDB_BLCKSZ / 2)) / tuple_width;
	}
	*tuples = rint(density * (double) curpages);

	/*
	 * We use relallvisible as-is, rather than scaling it up like we do for
	 * the pages and tuples counts, on the theory that any pages added since
	 * the last VACUUM are most likely not marked all-visible.  But costsize.c
	 * wants it converted to a fraction.
	 */
	if (relallvisible == 0 || curpages <= 0)
		*allvisfrac = 0;
	else if ((double) relallvisible >= curpages)
		*allvisfrac = 1;
	else
		*allvisfrac = (double) relallvisible / curpages;
}


/* ------------------------------------------------------------------------
 * Executor related callbacks for the heap AM
 * ------------------------------------------------------------------------
 */

static bool
orioledb_scan_bitmap_next_block(TableScanDesc scan,
								TBMIterateResult *tbmres)
{
	elog(ERROR, "Not implemented: %s", PG_FUNCNAME_MACRO);
	return false;
}

static bool
orioledb_scan_bitmap_next_tuple(TableScanDesc scan,
								TBMIterateResult *tbmres,
								TupleTableSlot *slot)
{
	elog(ERROR, "Not implemented: %s", PG_FUNCNAME_MACRO);
	return false;
}

static bool
orioledb_scan_sample_next_block(TableScanDesc scan, SampleScanState *scanstate)
{
	elog(ERROR, "Not implemented: %s", PG_FUNCNAME_MACRO);
	return false;
}

static bool
orioledb_scan_sample_next_tuple(TableScanDesc scan, SampleScanState *scanstate,
								TupleTableSlot *slot)
{
	elog(ERROR, "Not implemented: %s", PG_FUNCNAME_MACRO);
	return false;
}

static TableScanDesc
orioledb_beginscan(Relation relation, Snapshot snapshot,
				   int nkeys, ScanKey key,
				   ParallelTableScanDesc parallel_scan,
				   uint32 flags)
{
	OTableDescr *descr;
	OScanDesc	scan;

	descr = relation_get_descr(relation);

	/*
	 * allocate and initialize scan descriptor
	 */
	scan = (OScanDesc) palloc(sizeof(OScanDescData));

	scan->rs_base.rs_rd = relation;
	scan->rs_base.rs_snapshot = snapshot;
	scan->rs_base.rs_nkeys = nkeys;
	scan->rs_base.rs_flags = flags;
	scan->rs_base.rs_parallel = parallel_scan;

	if (nkeys > 0)
	{
		scan->rs_base.rs_key = (ScanKey) palloc(sizeof(ScanKeyData) * nkeys);
		memcpy(scan->rs_base.rs_key, key, sizeof(ScanKeyData) * nkeys);
	}
	else
	{
		scan->rs_base.rs_key = NULL;
	}

	if (scan->rs_base.rs_flags & SO_TYPE_ANALYZE)
		scan->csn = COMMITSEQNO_INPROGRESS;
	else
		scan->csn = snapshot->snapshotcsn;

	ItemPointerSetBlockNumber(&scan->iptr, 0);
	ItemPointerSetOffsetNumber(&scan->iptr, FirstOffsetNumber);

	if (descr)
	{
		o_btree_load_shmem(&GET_PRIMARY(descr)->desc);
		scan->scan = make_btree_seq_scan(&GET_PRIMARY(descr)->desc, scan->csn);
	}

	return &scan->rs_base;
}

static void
orioledb_rescan(TableScanDesc sscan, ScanKey key, bool set_params,
				bool allow_strat, bool allow_sync, bool allow_pagemode)
{
	OTableDescr *descr;
	OScanDesc	scan;

	scan = (OScanDesc) sscan;
	descr = relation_get_descr(scan->rs_base.rs_rd);

	memcpy(scan->rs_base.rs_key, key, sizeof(ScanKeyData) *
		   scan->rs_base.rs_nkeys);

	if (scan->scan)
		free_btree_seq_scan(scan->scan);

	o_btree_load_shmem(&GET_PRIMARY(descr)->desc);
	scan->scan = make_btree_seq_scan(&GET_PRIMARY(descr)->desc, scan->csn);
}

static void
orioledb_endscan(TableScanDesc sscan)
{
	OScanDesc	scan = (OScanDesc) sscan;

	STOPEVENT(STOPEVENT_SCAN_END, NULL);

	if (scan->scan)
		free_btree_seq_scan(scan->scan);
}

static bool
slot_keytest(TupleTableSlot *slot, int nkeys, ScanKey keys)
{
	int			i;
	ScanKey		key;

	for (i = 0; i < nkeys; i++)
	{
		Datum		val;
		bool		isnull;
		Datum		test;

		key = keys + i;

		if (key->sk_flags & SK_ISNULL)
			return false;

		val = slot_getattr(slot, key->sk_attno, &isnull);

		if (isnull)
			return false;

		test = FunctionCall2Coll(&key->sk_func,
								 key->sk_collation,
								 val,
								 key->sk_argument);

		if (!DatumGetBool(test))
			return false;
	}

	return true;
}

static bool
orioledb_getnextslot(TableScanDesc sscan, ScanDirection direction,
					 TupleTableSlot *slot)
{
	OScanDesc	scan;
	OTableDescr *descr;
	bool		result;

	do
	{
		OTuple		tuple;
		BTreeLocationHint hint;
		CommitSeqNo tupleCsn;

		scan = (OScanDesc) sscan;
		descr = relation_get_descr(scan->rs_base.rs_rd);

		tuple = btree_seq_scan_getnext(scan->scan, slot->tts_mcxt,
									   &tupleCsn, &hint);

		if (O_TUPLE_IS_NULL(tuple))
			return false;

		tts_orioledb_store_tuple(slot, tuple, descr, tupleCsn,
								 PrimaryIndexNumber, true, &hint);

		result = slot_keytest(slot,
							  scan->rs_base.rs_nkeys,
							  scan->rs_base.rs_key);
	}
	while (!result);

	return true;
}

/*
 * partial (and parallel) scans disabled by orioledb_set_rel_pathlist_hook()
 */
static Size
orioledb_parallelscan_estimate(Relation rel)
{
	elog(ERROR, "Not implemented: %s", PG_FUNCNAME_MACRO);
	return 0;
}

static Size
orioledb_parallelscan_initialize(Relation rel, ParallelTableScanDesc pscan)
{
	elog(ERROR, "Not implemented: %s", PG_FUNCNAME_MACRO);
	return 0;
}

static void
orioledb_parallelscan_reinitialize(Relation rel,
								   ParallelTableScanDesc pscan)
{
	elog(ERROR, "Not implemented: %s", PG_FUNCNAME_MACRO);
}

static void
orioledb_tableam_multi_insert(Relation relation, TupleTableSlot **slots,
							  int ntuples, CommandId cid,
							  int options, BulkInsertState bistate)
{
	elog(ERROR, "Not implemented: %s", PG_FUNCNAME_MACRO);
}

static void
orioledb_multi_insert(Relation relation, TupleTableSlot **slots, int ntuples,
					  EState *estate,
					  CommandId cid, int options, BulkInsertState bistate)
{
	int			i;

	for (i = 0; i < ntuples; i++)
		orioledb_tuple_insert(relation, slots[i], estate,
							  cid, options, bistate);
}

static void
orioledb_get_latest_tid(TableScanDesc sscan,
						ItemPointer tid)
{
	elog(ERROR, "Not implemented: %s", PG_FUNCNAME_MACRO);
}

static void
orioledb_vacuum_rel(Relation onerel, VacuumParams *params,
					BufferAccessStrategy bstrategy)
{
	/* nothing to do */
}

#if PG_VERSION_NUM >= 140000
static TransactionId
orioledb_index_delete_tuples(Relation rel, TM_IndexDeleteOp *delstate)
{
	elog(ERROR, "Not implemented");
}
#else
static TransactionId
orioledb_compute_xid_horizon_for_tuples(Relation rel,
										ItemPointerData *tids,
										int nitems)
{
	elog(ERROR, "Not implemented");
	return 0;
}
#endif

static void
orioledb_init_modify(ModifyTableState *mstate, ResultRelInfo *rinfo)
{
	OTableDescr *descr = relation_get_descr(rinfo->ri_RelationDesc);
	ModifyTable *node = (ModifyTable *) mstate->ps.plan;
	OFDWState  *state;

	state = (OFDWState *) palloc0(sizeof(OFDWState));
	rinfo->ri_FdwState = state;
	state->conflictIxOid = InvalidOid;

	if (mstate->operation == CMD_INSERT &&
		node->onConflictAction != ONCONFLICT_NONE)
	{
		List	   *arbiterIndexes;
		Oid			arbiterOid;
		int			i;

		arbiterIndexes = rinfo->ri_onConflictArbiterIndexes;

		if (arbiterIndexes == NIL)
		{
			state->conflictIxOid = InvalidOid;
		}
		else
		{
			arbiterOid = list_nth_oid(arbiterIndexes, 0);

			for (i = 0; i < descr->nIndices; i++)
			{
				if (descr->indices[i]->oids.reloid == arbiterOid)
				{
					state->conflictIxOid = arbiterOid;
					break;
				}
			}
		}

		if (state->conflictIxOid < 0)
			elog(ERROR, "Arbiter index isn't matched");
	}
}

static bool
orioledb_rewrite_table(Relation old_rel)
{
	bool		result = false;

	ORelOids	oids;
	OTable	   *old_o_table,
			   *o_table;
	OTableDescr *old_descr,
				tmp_descr;

	oids.datoid = MyDatabaseId;
	oids.reloid = old_rel->rd_id;
	oids.relnode = old_rel->rd_node.relNode;
	o_table = o_tables_get(oids);

	if (o_table == NULL)
	{
		/* it does not exist */
		elog(ERROR, "orioledb table \"%s\" not found",
			 RelationGetRelationName(old_rel));
	}

	old_o_table = o_table;
	o_table = o_tables_get(o_table->oids);
	if (o_table == NULL)
	{
		/* it does not exist */
		elog(ERROR, "orioledb table \"%s\" not found",
			 RelationGetRelationName(old_rel));
	}
	assign_new_oids(o_table, old_rel);

	o_table->primary_init_nfields = old_o_table->nfields;

	LWLockAcquire(&checkpoint_state->oTablesAddLock, LW_SHARED);
	old_descr = o_fetch_table_descr(old_o_table->oids);

	o_fill_tmp_table_descr(&tmp_descr, o_table);
	rebuild_indices(old_o_table, old_descr, o_table, &tmp_descr);
	o_free_tmp_table_descr(&tmp_descr);

	recreate_o_table(old_o_table, o_table);

	o_table_free(old_o_table);
	o_table_free(o_table);
	LWLockRelease(&checkpoint_state->oTablesAddLock);

	result = true;

	return result;
}

void
orioledb_free_rd_amcache(Relation rel)
{
	if (rel->rd_amcache)
		table_descr_dec_refcnt((OTableDescr *) rel->rd_amcache);
	rel->rd_amcache = NULL;
}

static int
orioledb_acquire_sample_rows(Relation relation, int elevel,
							 HeapTuple *rows, int targrows,
							 double *totalrows,
							 double *totaldeadrows)
{
	OTableDescr *descr = relation_get_descr(relation);
	OIndexDescr *pk = GET_PRIMARY(descr);
	BTreeSeqScan *scan;
	BlockNumber nblocks;
	ReservoirStateData rstate;
	bool		scanEnd;
	OTuple		tuple;
	TupleTableSlot *slot = descr->newTuple;
	int			numrows = 0;	/* # rows now in reservoir */
	double		samplerows = 0; /* total # rows collected */
	double		liverows = 0;	/* # live rows seen */
	double		deadrows = 0;	/* # dead rows seen */
	double		rowstoskip = -1;	/* -1 means not set yet */
	BlockSamplerData bs;
	BlockNumber totalblocks = TREE_NUM_LEAF_PAGES(&pk->desc);

	nblocks = BlockSampler_Init(&bs, totalblocks,
								targrows, random());

	scan = make_btree_sampling_scan(&pk->desc, &bs);

	/* Report sampling block numbers */
	pgstat_progress_update_param(PROGRESS_ANALYZE_BLOCKS_TOTAL,
								 nblocks);

	/* Prepare for sampling rows */
	reservoir_init_selection_state(&rstate, targrows);

	tuple = btree_seq_scan_getnext_raw(scan, CurrentMemoryContext,
									   &scanEnd, NULL);
	while (!scanEnd)
	{
		tuple = btree_seq_scan_getnext_raw(scan, CurrentMemoryContext,
										   &scanEnd, NULL);

		if (!O_TUPLE_IS_NULL(tuple))
		{
			tts_orioledb_store_tuple(slot, tuple, descr, COMMITSEQNO_INPROGRESS,
									 PrimaryIndexNumber, false, NULL);

			liverows += 1;

			if (numrows < targrows)
				rows[numrows++] = ExecCopySlotHeapTuple(slot);
			else
			{
				/*
				 * t in Vitter's paper is the number of records already
				 * processed.  If we need to compute a new S value, we must
				 * use the not-yet-incremented value of samplerows as t.
				 */
				if (rowstoskip < 0)
					rowstoskip = reservoir_get_next_S(&rstate, samplerows, targrows);

				if (rowstoskip <= 0)
				{
					/*
					 * Found a suitable tuple, so save it, replacing one old
					 * tuple at random
					 */
#if PG_VERSION_NUM >= 150000
					int			k = (int) (targrows * sampler_random_fract(&rstate.randstate));
#else
					int			k = (int) (targrows * sampler_random_fract(rstate.randstate));
#endif
					Assert(k >= 0 && k < targrows);
					heap_freetuple(rows[k]);
					rows[k] = ExecCopySlotHeapTuple(slot);
				}

				rowstoskip -= 1;
			}
			samplerows += 1;
		}
		else
		{
			deadrows += 1;
		}
	}
	free_btree_seq_scan(scan);

	/*
	 * Estimate total numbers of live and dead rows in relation, extrapolating
	 * on the assumption that the average tuple density in pages we didn't
	 * scan is the same as in the pages we did scan.  Since what we scanned is
	 * a random sample of the pages in the relation, this should be a good
	 * assumption.
	 */
	if (bs.m > 0)
	{
		*totalrows = floor((liverows / bs.m) * totalblocks + 0.5);
		*totaldeadrows = floor((deadrows / bs.m) * totalblocks + 0.5);
	}
	else
	{
		*totalrows = 0.0;
		*totaldeadrows = 0.0;
	}

	/*
	 * Emit some interesting relation info
	 */
	ereport(elevel,
			(errmsg("\"%s\": scanned %d of %u pages, "
					"containing %.0f live rows and %.0f dead rows; "
					"%d rows in sample, %.0f estimated total rows",
					RelationGetRelationName(relation),
					bs.m, totalblocks,
					liverows, deadrows,
					numrows, *totalrows)));

	return numrows;
}

static void
orioledb_analyze_table(Relation relation,
					   AcquireSampleRowsFunc *func,
					   BlockNumber *totalpages)
{
	OTableDescr *descr = relation_get_descr(relation);
	OIndexDescr *pk = GET_PRIMARY(descr);

	o_btree_load_shmem(&pk->desc);

	*func = orioledb_acquire_sample_rows;
	*totalpages = TREE_NUM_LEAF_PAGES(&pk->desc);
}


/* ------------------------------------------------------------------------
 * Definition of the orioledb table access method.
 * ------------------------------------------------------------------------
 */

static const ExtendedTableAmRoutine orioledb_am_methods = {
	.tableam = {.type = T_ExtendedTableAmRoutine,
		.slot_callbacks = orioledb_slot_callbacks,

		.scan_begin = orioledb_beginscan,
		.scan_end = orioledb_endscan,
		.scan_rescan = orioledb_rescan,
		.scan_getnextslot = orioledb_getnextslot,

		.parallelscan_estimate = orioledb_parallelscan_estimate,
		.parallelscan_initialize = orioledb_parallelscan_initialize,
		.parallelscan_reinitialize = orioledb_parallelscan_reinitialize,

		.index_fetch_begin = orioledb_index_fetch_begin,
		.index_fetch_reset = orioledb_index_fetch_reset,
		.index_fetch_end = orioledb_index_fetch_end,
		.index_fetch_tuple = orioledb_index_fetch_tuple,
#if PG_VERSION_NUM >= 140000
		.index_delete_tuples = orioledb_index_delete_tuples,
#endif

		.tuple_insert = orioledb_tableam_tuple_insert,
		.tuple_insert_speculative = orioledb_tuple_insert_speculative,
		.tuple_complete_speculative = orioledb_tuple_complete_speculative,
		.multi_insert = orioledb_tableam_multi_insert,
		.tuple_delete = orioledb_tableam_tuple_delete,
		.tuple_update = orioledb_tableam_tuple_update,
		.tuple_lock = orioledb_tableam_tuple_lock,
		.finish_bulk_insert = orioledb_finish_bulk_insert,

		.tuple_fetch_row_version = orioledb_tableam_fetch_row_version,
		.tuple_get_latest_tid = orioledb_get_latest_tid,
		.tuple_tid_valid = orioledb_tuple_tid_valid,
		.tuple_satisfies_snapshot = orioledb_tuple_satisfies_snapshot,
#if PG_VERSION_NUM < 140000
		.compute_xid_horizon_for_tuples = orioledb_compute_xid_horizon_for_tuples,
#endif

		.relation_set_new_filenode = orioledb_relation_set_new_filenode,
		.relation_nontransactional_truncate = orioledb_relation_nontransactional_truncate,
		.relation_copy_data = orioledb_relation_copy_data,
		.relation_copy_for_cluster = orioledb_relation_copy_for_cluster,
		.relation_vacuum = orioledb_vacuum_rel,
		.scan_analyze_next_block = orioledb_scan_analyze_next_block,
		.scan_analyze_next_tuple = orioledb_scan_analyze_next_tuple,
		.index_build_range_scan = orioledb_index_build_range_scan,
		.index_validate_scan = orioledb_index_validate_scan,

		.relation_size = orioledb_relation_size,
		.relation_needs_toast_table = orioledb_relation_needs_toast_table,
		.relation_toast_am = orioledb_relation_toast_am,

		.relation_estimate_size = orioledb_estimate_rel_size,
		.scan_bitmap_next_block = orioledb_scan_bitmap_next_block,
		.scan_bitmap_next_tuple = orioledb_scan_bitmap_next_tuple,
		.scan_sample_next_block = orioledb_scan_sample_next_block,
		.scan_sample_next_tuple = orioledb_scan_sample_next_tuple
	},
	.get_row_ref_type = orioledb_get_row_ref_type,
	.tuple_insert = orioledb_tuple_insert,
	.multi_insert = orioledb_multi_insert,
	.tuple_insert_on_conflict = orioledb_tuple_insert_on_conflict,
	.tuple_delete = orioledb_tuple_delete,
	.tuple_update = orioledb_tuple_update,
	.tuple_lock = orioledb_tuple_lock,
	.tuple_fetch_row_version = orioledb_fetch_row_version,
	.tuple_refetch_row_version = orioledb_refetch_row_version,
	.init_modify = orioledb_init_modify,
	.rewrite_table = orioledb_rewrite_table,
	.free_rd_amcache = orioledb_free_rd_amcache,
	.analyze_table = orioledb_analyze_table
};

bool
is_orioledb_rel(Relation rel)
{
	return (rel->rd_tableam == (TableAmRoutine *) &orioledb_am_methods);
}

Datum		orioledb_tableam_handler(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(orioledb_tableam_handler);

Datum
orioledb_tableam_handler(PG_FUNCTION_ARGS)
{
	orioledb_check_shmem();

	PG_RETURN_POINTER(&orioledb_am_methods);
}

/*
 * Returns private descriptor for relation
 *
 * In order to save some hash lookup, we cache descriptor in rel->rd_amcache.
 * Since rel->rd_amcache is automatically freed on cache invalidation, we
 * can't set rel->rd_amcache to the descriptor directly.  But we may use
 * pointer to allocated area contained pointer to descriptor.
 */
OTableDescr *
relation_get_descr(Relation rel)
{
	OTableDescr *result;
	ORelOids	oids = {MyDatabaseId, RelationGetRelid(rel), rel->rd_node.relNode};

	if (!is_orioledb_rel(rel))
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not a orioledb table", NameStr(rel->rd_rel->relname))));

	if (rel->rd_amcache)
		return (OTableDescr *) rel->rd_amcache;

	result = o_fetch_table_descr(oids);
	rel->rd_amcache = result;
	if (result)
		table_descr_inc_refcnt(result);
	return result;
}

static void
get_keys_from_rowid(OIndexDescr *id, Datum pkDatum, OBTreeKeyBound *key,
					BTreeLocationHint *hint, CommitSeqNo *csn, uint32 *version)
{
	bytea	   *rowid;
	Pointer		p;

	key->nkeys = id->nonLeafTupdesc->natts;

	if (!id->primaryIsCtid)
	{
		OTuple		tuple;
		ORowIdAddendumNonCtid *add;

		rowid = DatumGetByteaP(pkDatum);
		p = (Pointer) rowid + MAXALIGN(VARHDRSZ);
		add = (ORowIdAddendumNonCtid *) p;
		p += MAXALIGN(sizeof(ORowIdAddendumNonCtid));

		tuple.data = p;
		tuple.formatFlags = add->flags;
		*hint = add->hint;
		if (csn)
			*csn = add->csn;
		if (version)
			*version = o_tuple_get_version(tuple);
		o_fill_key_bound(id, tuple, BTreeKeyNonLeafKey, key);
	}
	else
	{
		ORowIdAddendumCtid *add;

		rowid = DatumGetByteaP(pkDatum);
		p = (Pointer) rowid + MAXALIGN(VARHDRSZ);
		add = (ORowIdAddendumCtid *) p;
		*hint = add->hint;
		if (csn)
			*csn = add->csn;
		if (version)
			*version = add->version;
		p += MAXALIGN(sizeof(ORowIdAddendumCtid));

		key->keys[0].value = PointerGetDatum(p);
		key->keys[0].type = TIDOID;
		key->keys[0].flags = O_VALUE_BOUND_LOWER | O_VALUE_BOUND_INCLUSIVE | O_VALUE_BOUND_COERCIBLE;
		key->keys[0].comparator = NULL;
	}
}

void
o_check_constraints(ResultRelInfo *rinfo, TupleTableSlot *slot, EState *estate)
{
	/*
	 * Check the constraints of the tuple.
	 */
	if (rinfo->ri_RelationDesc->rd_att->constr)
		ExecConstraints(rinfo, slot, estate);

	/*
	 * Also check the tuple against the partition constraint, if there is one;
	 * except that if we got here via tuple-routing, we don't need to if
	 * there's no BR trigger defined on the partition.
	 */
	if (rinfo->ri_RelationDesc->rd_rel->relispartition &&
		(rinfo->ri_RootResultRelInfo == NULL ||
		 (rinfo->ri_TrigDesc &&
		  rinfo->ri_TrigDesc->trig_insert_before_row)))
		ExecPartitionCheck(rinfo, slot, estate, true);
}
