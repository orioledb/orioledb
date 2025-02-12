/*-------------------------------------------------------------------------
 *
 * handler.c
 *		Implementation of table access method handler
 *
 * Copyright (c) 2021-2025, Oriole DB Inc.
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
#include "catalog/o_indices.h"
#include "catalog/o_tables.h"
#include "catalog/o_sys_cache.h"
#include "recovery/wal.h"
#include "tableam/descr.h"
#include "tableam/handler.h"
#include "tableam/operations.h"
#include "tableam/tree.h"
#include "transam/oxid.h"
#include "tuple/slot.h"
#include "utils/compress.h"
#include "utils/rel.h"
#include "utils/stopevent.h"

#include "access/heapam.h"
#include "access/heaptoast.h"
#include "access/multixact.h"
#include "access/reloptions.h"
#include "access/tableam.h"
#include "catalog/heap.h"
#include "catalog/index.h"
#include "catalog/namespace.h"
#include "catalog/pg_am.h"
#include "catalog/pg_collation.h"
#include "catalog/storage.h"
#include "commands/progress.h"
#include "commands/vacuum.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "optimizer/optimizer.h"
#include "optimizer/plancat.h"
#include "parser/parse_coerce.h"
#include "parser/parse_relation.h"
#include "parser/parse_type.h"
#include "parser/parsetree.h"
#include "storage/bufmgr.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/backend_progress.h"
#include "utils/datum.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/sampling.h"
#include "utils/syscache.h"

bool		in_nontransactional_truncate = false;

typedef struct OScanDescData
{
	TableScanDescData rs_base;	/* AM independent part of the descriptor */
	BTreeSeqScan *scan;
	OSnapshot	o_snapshot;
	ItemPointerData iptr;
} OScanDescData;
typedef OScanDescData *OScanDesc;

/*
 * Operation with indices. It does not update TOAST BTree. Implementations
 * are in tableam_handler.c.
 */
static void get_keys_from_rowid(OIndexDescr *id, Datum pkDatum, OBTreeKeyBound *key,
								BTreeLocationHint *hint, CommitSeqNo *csn,
								uint32 *version);
static void rowid_set_csn(OIndexDescr *id, Datum pkDatum, CommitSeqNo csn);


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
 * Index Scan Callbacks for orioledb AM
 * ------------------------------------------------------------------------
 */

/*
 * Descriptor for fetches from orioledb table.
 */
typedef struct OrioledbIndexFetchData
{
	IndexFetchTableData xs_base;	/* AM independent part of the descriptor */
} OrioledbIndexFetchData;

/*
 * Returns NULL to prevent index scan from inside of standard_planner
 * for greater and lower where clauses.
 */
static IndexFetchTableData *
orioledb_index_fetch_begin(Relation rel)
{
	OrioledbIndexFetchData *o_scan = palloc0(sizeof(OrioledbIndexFetchData));


	o_scan->xs_base.rel = rel;

	return &o_scan->xs_base;
}

static void
orioledb_index_fetch_reset(IndexFetchTableData *scan)
{
}

static void
orioledb_index_fetch_end(IndexFetchTableData *scan)
{
	OrioledbIndexFetchData *o_scan = (OrioledbIndexFetchData *) scan;

	orioledb_index_fetch_reset(scan);

	pfree(o_scan);
}

static bool
orioledb_index_fetch_tuple(struct IndexFetchTableData *scan,
						   Datum tupleid,
						   Snapshot snapshot,
						   TupleTableSlot *slot,
						   bool *call_again, bool *all_dead)
{
	OTableDescr *descr;
	OBTreeKeyBound pkey;
	OTuple		tuple;
	BTreeLocationHint hint;
	CommitSeqNo csn;
	OSnapshot	oSnapshot;
	CommitSeqNo tupleCsn;
	uint32		version;

	Assert(slot->tts_ops == &TTSOpsOrioleDB);

	*call_again = false;
	*all_dead = false;

	descr = relation_get_descr(scan->rel);
	Assert(descr != NULL);

	if (GET_PRIMARY(descr)->primaryIsCtid)
		o_btree_load_shmem(&GET_PRIMARY(descr)->desc);

	get_keys_from_rowid(GET_PRIMARY(descr), tupleid, &pkey, &hint,
						&csn, &version);
	O_LOAD_SNAPSHOT_CSN(&oSnapshot, csn);

	tuple = o_btree_find_tuple_by_key(&GET_PRIMARY(descr)->desc,
									  (Pointer) &pkey,
									  BTreeKeyBound,
									  &oSnapshot, &tupleCsn,
									  slot->tts_mcxt,
									  &hint);

	if (O_TUPLE_IS_NULL(tuple))
		return false;

	tts_orioledb_store_tuple(slot, tuple, descr, tupleCsn,
							 PrimaryIndexNumber, true, &hint);
	slot->tts_tableOid = descr->oids.reloid;

	/* FIXME? */
	if (snapshot->snapshot_type == SNAPSHOT_DIRTY)
		snapshot->xmin = snapshot->xmax = InvalidTransactionId;

	return true;
}


/* ------------------------------------------------------------------------
 * Callbacks for non-modifying operations on individual tuples for heap AM
 * ------------------------------------------------------------------------
 */

static TupleFetchCallbackResult
fetch_row_version_callback(OTuple tuple, OXid tupOxid, OSnapshot *oSnapshot,
						   void *arg, TupleFetchCallbackCheckType check_type)
{
	uint32		version = *((uint32 *) arg);

	if (check_type != OTupleFetchCallbackVersionCheck)
		return OTupleFetchNext;

	if (!(COMMITSEQNO_IS_INPROGRESS(oSnapshot->csn) &&
		  tupOxid == get_current_oxid_if_any()))
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
	CommitSeqNo csn;
	OSnapshot	oSnapshot;
	CommitSeqNo tupleCsn;
	uint32		version;
	bool		deleted;

	descr = relation_get_descr(relation);

	Assert(descr != NULL);
	if (GET_PRIMARY(descr)->primaryIsCtid)
		o_btree_load_shmem(&GET_PRIMARY(descr)->desc);

	get_keys_from_rowid(GET_PRIMARY(descr), tupleid, &pkey, &hint,
						&csn, &version);
	O_LOAD_SNAPSHOT_CSN(&oSnapshot, csn);

	tuple = o_btree_find_tuple_by_key_cb(&GET_PRIMARY(descr)->desc,
										 (Pointer) &pkey,
										 BTreeKeyBound,
										 &oSnapshot, &tupleCsn,
										 slot->tts_mcxt,
										 &hint,
										 &deleted,
										 fetch_row_version_callback,
										 &version);

	if (deleted && COMMITSEQNO_IS_INPROGRESS(tupleCsn) && snapshot != SnapshotAny)
		return true;

	if (O_TUPLE_IS_NULL(tuple))
		return false;

	tts_orioledb_store_tuple(slot, tuple, descr, tupleCsn,
							 PrimaryIndexNumber, true, &hint);
	slot->tts_tableOid = RelationGetRelid(relation);

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
	OBTreeKeyBound pkey;
	OTableDescr *descr;
	OTuple		tuple;
	CommitSeqNo tupleCsn;
	BTreeLocationHint hint = {OInvalidInMemoryBlkno, 0};

	if (snapshot != SnapshotSelf)
		return true;

	descr = relation_get_descr(rel);

	Assert(descr != NULL);
	if (GET_PRIMARY(descr)->primaryIsCtid)
		o_btree_load_shmem(&GET_PRIMARY(descr)->desc);

	tts_orioledb_fill_key_bound(slot, GET_PRIMARY(descr), &pkey);

	tuple = o_btree_find_tuple_by_key(&GET_PRIMARY(descr)->desc,
									  (Pointer) &pkey,
									  BTreeKeyBound,
									  &o_in_progress_snapshot,
									  &tupleCsn,
									  slot->tts_mcxt,
									  &hint);

	if (O_TUPLE_IS_NULL(tuple))
		return false;

	tts_orioledb_store_tuple(slot, tuple, descr, tupleCsn,
							 PrimaryIndexNumber, true, &hint);
	slot->tts_tableOid = RelationGetRelid(rel);

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

static TupleTableSlot *
orioledb_tuple_insert(Relation relation, TupleTableSlot *slot,
					  CommandId cid, int options, BulkInsertState bistate)
{
	OTableDescr *descr;
	OSnapshot	oSnapshot;
	OXid		oxid;

	if (OidIsValid(relation->rd_rel->relrewrite))
		return slot;

	descr = relation_get_descr(relation);
	fill_current_oxid_osnapshot(&oxid, &oSnapshot);
	return o_tbl_insert(descr, relation, slot, oxid, oSnapshot.csn);
}

static TupleTableSlot *
orioledb_tuple_insert_with_arbiter(ResultRelInfo *rinfo,
								   TupleTableSlot *slot,
								   CommandId cid, int options,
								   struct BulkInsertStateData *bistate,
								   List *arbiterIndexes,
								   EState *estate,
								   LockTupleMode lockmode,
								   TupleTableSlot *lockedSlot,
								   TupleTableSlot *tempSlot)
{
	Relation	rel = rinfo->ri_RelationDesc;
	OTableDescr *descr;
	OTuple		tup;
	OIndexDescr *id;

	descr = relation_get_descr(rel);
	Assert(descr);
	id = GET_PRIMARY(descr);

	if (slot->tts_ops != descr->newTuple->tts_ops)
	{
		ExecCopySlot(descr->newTuple, slot);
		slot = descr->newTuple;
	}

	Assert(slot->tts_ops == &TTSOpsOrioleDB);

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

	slot = o_tbl_insert_with_arbiter(rel, descr, slot, arbiterIndexes,
									 lockmode, lockedSlot);

	return slot;
}

static TM_Result
orioledb_tuple_delete(Relation relation, Datum tupleid, CommandId cid,
					  Snapshot snapshot, Snapshot crosscheck, int options,
					  TM_FailureData *tmfd, bool changingPart,
					  TupleTableSlot *oldSlot)
{
	OModifyCallbackArg marg;
	OTableModifyResult mres;
	OBTreeKeyBound pkey;
	OTableDescr *descr;
	OXid		oxid;
	BTreeLocationHint hint;
	OSnapshot	oSnapshot;

	if (snapshot)
		O_LOAD_SNAPSHOT(&oSnapshot, snapshot);
	else
		oSnapshot = o_in_progress_snapshot;

	descr = relation_get_descr(relation);

	Assert(descr != NULL);
	if (GET_PRIMARY(descr)->primaryIsCtid)
		o_btree_load_shmem(&GET_PRIMARY(descr)->desc);

	oxid = get_current_oxid();

	marg.descr = descr;
	marg.oxid = oxid;
	marg.options = options;
	marg.scanSlot = oldSlot;
	marg.tmpSlot = descr->oldTuple;
	marg.modified = false;
	marg.deleted = BTreeLeafTupleNonDeleted;
	marg.changingPart = changingPart;
	marg.keyAttrs = NULL;

	get_keys_from_rowid(GET_PRIMARY(descr), tupleid, &pkey, &hint, &marg.csn, NULL);

	mres = o_tbl_delete(descr, &pkey, oxid, oSnapshot.csn, &hint, &marg);

	if (mres.self_modified)
	{
		tmfd->xmax = GetCurrentTransactionId();
		tmfd->cmax = GetCurrentCommandId(true);
		return TM_SelfModified;
	}

	if (marg.modified)
	{
		rowid_set_csn(GET_PRIMARY(descr), tupleid, marg.csn);
		tmfd->traversed = true;
	}
	else
	{
		tmfd->traversed = false;
	}

	if (marg.deleted == BTreeLeafTupleMovedPartitions)
	{
		tmfd->traversed = true;
		ItemPointerSetMovedPartitions(&tmfd->ctid);
		return TM_Updated;
	}
	else
	{
		ASAN_UNPOISON_MEMORY_REGION(&tmfd->ctid, sizeof(tmfd->ctid));
		ItemPointerSet(&tmfd->ctid, 0, FirstOffsetNumber);
	}

	if (mres.success && mres.action == BTreeOperationLock)
		return TM_Updated;

	o_check_tbl_delete_mres(mres, descr, relation);

	tmfd->xmax = InvalidTransactionId;
	tmfd->cmax = InvalidCommandId;

	if (mres.success)
	{
		return mres.self_modified ? TM_SelfModified : TM_Ok;
	}

	return mres.self_modified ? TM_SelfModified : (marg.modified ? TM_Updated : TM_Deleted);
}

static TM_Result
orioledb_tuple_update(Relation relation, Datum tupleid, TupleTableSlot *slot,
					  CommandId cid, Snapshot snapshot, Snapshot crosscheck,
					  int options, TM_FailureData *tmfd,
					  LockTupleMode *lockmode,
#if PG_VERSION_NUM >= 160000
					  TU_UpdateIndexes *update_indexes,
#else
					  bool *update_indexes,
#endif
					  TupleTableSlot *oldSlot)
{
	OTableModifyResult mres;
	OModifyCallbackArg marg;
	OBTreeKeyBound old_pkey;
	OTableDescr *descr;
	OXid		oxid;
	BTreeLocationHint hint;
	OSnapshot	oSnapshot;

	if (snapshot)
		O_LOAD_SNAPSHOT(&oSnapshot, snapshot);
	else
		oSnapshot = o_in_progress_snapshot;

	descr = relation_get_descr(relation);

	Assert(descr != NULL);
	if (GET_PRIMARY(descr)->primaryIsCtid)
		o_btree_load_shmem(&GET_PRIMARY(descr)->desc);

#if PG_VERSION_NUM >= 160000
	*update_indexes = TU_All;
#else
	*update_indexes = true;
#endif
	oxid = get_current_oxid();

	get_keys_from_rowid(GET_PRIMARY(descr), tupleid, &old_pkey, &hint,
						&marg.csn, NULL);
	if (slot->tts_ops != descr->newTuple->tts_ops)
	{
		ExecCopySlot(descr->newTuple, slot);
		slot = descr->newTuple;
	}

	marg.descr = descr;
	marg.oxid = oxid;
	marg.options = options;
	marg.scanSlot = oldSlot;
	marg.tmpSlot = descr->oldTuple;
	marg.modified = false;
	marg.deleted = BTreeLeafTupleNonDeleted;
	marg.newSlot = (OTableSlot *) slot;
	marg.keyAttrs = RelationGetIndexAttrBitmap(relation,
											   INDEX_ATTR_BITMAP_KEY);

	mres = o_tbl_update(descr, slot, &old_pkey, relation, oxid,
						oSnapshot.csn, &hint, &marg);

	if (mres.self_modified)
	{
		tmfd->xmax = GetCurrentTransactionId();
		tmfd->cmax = GetCurrentCommandId(true);
		return TM_SelfModified;
	}

	if (marg.modified)
	{
		rowid_set_csn(GET_PRIMARY(descr), tupleid, marg.csn);
		tmfd->traversed = true;
	}
	else
		tmfd->traversed = false;

	if (marg.deleted == BTreeLeafTupleMovedPartitions)
	{
		tmfd->traversed = true;
		ItemPointerSetMovedPartitions(&tmfd->ctid);
		return TM_Updated;
	}
	else
	{
		ASAN_UNPOISON_MEMORY_REGION(&tmfd->ctid, sizeof(tmfd->ctid));
		ItemPointerSet(&tmfd->ctid, 0, FirstOffsetNumber);
	}

	if (mres.success && mres.action == BTreeOperationLock)
	{
		if (TupIsNull(oldSlot))
			/* Tuple not passing quals anymore, exiting... */
			return TM_Deleted;

		return TM_Updated;
	}

	tmfd->xmax = InvalidTransactionId;
	tmfd->cmax = InvalidCommandId;
	o_check_tbl_update_mres(mres, descr, relation, slot);

	bms_free(marg.keyAttrs);
	Assert(mres.success);

	return mres.oldTuple ? TM_Ok : (marg.modified ? TM_Updated : TM_Deleted);
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
	larg.scanSlot = slot;
	larg.waitPolicy = wait_policy;
	larg.wouldBlock = false;
	larg.modified = false;
	larg.selfModified = false;
	larg.deleted = false;

	get_keys_from_rowid(GET_PRIMARY(descr), tupleid, &pkey, &hint, &larg.csn, NULL);

	res = o_tbl_lock(descr, &pkey, mode, oxid, &larg, &hint);

	if (larg.modified)
	{
		rowid_set_csn(GET_PRIMARY(descr), tupleid, larg.csn);
		tmfd->traversed = true;
	}
	else
		tmfd->traversed = false;

	if (larg.selfModified)
	{
		tmfd->xmax = GetCurrentTransactionId();
		tmfd->cmax = GetCurrentCommandId(true);
		return TM_SelfModified;
	}
	else
	{
		tmfd->xmax = InvalidTransactionId;
		tmfd->cmax = InvalidCommandId;
	}

	if (larg.deleted == BTreeLeafTupleMovedPartitions)
	{
		tmfd->traversed = true;
		ItemPointerSetMovedPartitions(&tmfd->ctid);
		return TM_Updated;
	}
	else
	{
		ASAN_UNPOISON_MEMORY_REGION(&tmfd->ctid, sizeof(tmfd->ctid));
		ItemPointerSet(&tmfd->ctid, 0, FirstOffsetNumber);
	}

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
		OTable	   *old_o_table,
				   *new_o_table;
		TupleDesc	tupdesc;
		OSnapshot	oSnapshot;
		OXid		oxid;
		int			oldTreeOidsNum,
					newTreeOidsNum;
		ORelOids	old_oids,
				   *oldTreeOids,
					new_oids,
				   *newTreeOids;

		ORelOidsSetFromRel(old_oids, rel);
		old_o_table = o_tables_get(old_oids);
		Assert(old_o_table != NULL);
		oldTreeOids = o_table_make_index_oids(old_o_table, &oldTreeOidsNum);

		tupdesc = RelationGetDescr(rel);
		ORelOidsSetFromRel(new_oids, rel);
		new_oids.relnode = RelFileNodeGetNode(newrnode);

		new_o_table = o_table_tableam_create(new_oids, tupdesc,
											 rel->rd_rel->relpersistence,
											 old_o_table->fillfactor);
		o_opclass_cache_add_table(new_o_table);
		o_table_fill_oids(new_o_table, rel, newrnode);

		newTreeOids = o_table_make_index_oids(new_o_table, &newTreeOidsNum);

		o_tables_table_meta_lock(new_o_table);

		fill_current_oxid_osnapshot(&oxid, &oSnapshot);
		o_tables_drop_by_oids(old_oids, oxid, oSnapshot.csn);
		o_tables_add(new_o_table, oxid, oSnapshot.csn);
		o_tables_table_meta_unlock(new_o_table, old_o_table->oids.relnode);
		o_table_free(new_o_table);

		orioledb_free_rd_amcache(rel);

		Assert(o_fetch_table_descr(new_oids) != NULL);
		add_undo_truncate_relnode(old_oids, oldTreeOids, oldTreeOidsNum,
								  new_oids, newTreeOids, newTreeOidsNum);
		pfree(oldTreeOids);
		pfree(newTreeOids);
	}

	*freezeXid = InvalidTransactionId;
	*minmulti = InvalidMultiXactId;

	srel = RelationCreateStorage(*newrnode, persistence, false);
	smgrclose(srel);
}

static void
drop_indices_for_rel(Relation rel, bool primary)
{
	ListCell   *index;
	Oid			indexOid;

	foreach(index, RelationGetIndexList(rel))
	{
		Relation	ind;
		bool		closed = false;

		indexOid = lfirst_oid(index);
		ind = relation_open(indexOid, AccessShareLock);

		if ((primary && ind->rd_index->indisprimary) || (!primary && !ind->rd_index->indisprimary))
		{
			OIndexNumber ix_num;
			OTableDescr *descr = relation_get_descr(rel);

			Assert(descr != NULL);
			ix_num = o_find_ix_num_by_name(descr, ind->rd_rel->relname.data);
			if (GET_PRIMARY(descr)->primaryIsCtid)
				ix_num--;
			relation_close(ind, AccessShareLock);
			o_index_drop(rel, ix_num);
			closed = true;
		}
		if (!closed)
			relation_close(ind, AccessShareLock);
	}
}

static void
orioledb_relation_nontransactional_truncate(Relation rel)
{
	ORelOids	oids;

	if (rel->rd_rel->relpersistence == RELPERSISTENCE_TEMP)
		in_nontransactional_truncate = true;

	ORelOidsSetFromRel(oids, rel);
	if (!OidIsValid(rel->rd_rel->oid) || rel->rd_rel->relkind == RELKIND_TOASTVALUE)
		return;

	o_truncate_table(oids);

	drop_indices_for_rel(rel, false);
	/* drop primary after all indices to not rebuild them */
	drop_indices_for_rel(rel, true);

	if (RelationIsPermanent(rel))
		add_truncate_wal_record(oids);
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
#if PG_VERSION_NUM >= 170000
orioledb_scan_analyze_next_block(TableScanDesc scan, ReadStream *stream)
#else
orioledb_scan_analyze_next_block(TableScanDesc scan, BlockNumber blockno,
								 BufferAccessStrategy bstrategy)
#endif
{
	OScanDesc	oscan = (OScanDesc) scan;
#if PG_VERSION_NUM >= 170000
	BufferAccessStrategy bstrategy = GetAccessStrategy(BAS_BULKREAD);
	BlockNumber blockno = read_stream_next_block(stream, &bstrategy);

	if (blockno == InvalidBlockNumber)
		return false;
#endif
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
			tts_orioledb_store_tuple(slot, tuple, descr, oscan->o_snapshot.csn,
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
	if (reltuples >= 0 && relpages > 0)
	{
		density = reltuples / (double) relpages;
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

Size
orioledb_parallelscan_estimate(Relation rel)
{
	if (!is_orioledb_rel(rel))
		ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
						errmsg("\"%s\" is not a orioledb table", NameStr(rel->rd_rel->relname))));

	return sizeof(ParallelOScanDescData);
}

static void
orioledb_parallelscan_initialize_internal(ParallelTableScanDesc pscan)
{
	ParallelOScanDesc poscan = (ParallelOScanDesc) pscan;

	clear_fixed_shmem_key(&poscan->intPage[0].prevHikey);
	clear_fixed_shmem_key(&poscan->intPage[1].prevHikey);
	memset(poscan->intPage[0].img, 0, ORIOLEDB_BLCKSZ);
	memset(poscan->intPage[1].img, 0, ORIOLEDB_BLCKSZ);
	poscan->intPage[0].status = OParallelScanPageInvalid;
	poscan->intPage[1].status = OParallelScanPageInvalid;
	poscan->intPage[0].startOffset = 0;
	poscan->intPage[1].startOffset = 0;
	poscan->intPage[0].offset = 0;
	poscan->intPage[1].offset = 0;
	poscan->downlinksCount = 0;
	poscan->workersReportedCount = 0;
	poscan->flags = 0;
	poscan->cur_int_pageno = 0;
	poscan->dsmHandle = 0;
	poscan->nworkers = 0;
#ifdef USE_ASSERT_CHECKING
	memset(poscan->worker_active, 0, sizeof(poscan->worker_active));
#endif
}

/* Modified copy of table_block_parallelscan_initialize */
Size
orioledb_parallelscan_initialize(Relation rel, ParallelTableScanDesc pscan)
{
	ParallelOScanDesc poscan = (ParallelOScanDesc) pscan;

	if (!is_orioledb_rel(rel))
		ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
						errmsg("\"%s\" is not a orioledb table", NameStr(rel->rd_rel->relname))));

	poscan->phs_base.phs_relid = RelationGetRelid(rel);
	poscan->phs_base.phs_syncscan = false;
	return orioledb_parallelscan_initialize_inner(pscan);
}

Size
orioledb_parallelscan_initialize_inner(ParallelTableScanDesc pscan)
{
	ParallelOScanDesc poscan = (ParallelOScanDesc) pscan;

	SpinLockInit(&poscan->intpageAccess);
	SpinLockInit(&poscan->workerStart);
	LWLockInitialize(&poscan->intpageLoad, btreeScanShmem->pageLoadTrancheId);
	LWLockInitialize(&poscan->downlinksPublish, btreeScanShmem->downlinksPublishTrancheId);
	pg_atomic_init_u64(&poscan->downlinkIndex, 0);

	orioledb_parallelscan_initialize_internal(pscan);

	return sizeof(ParallelOScanDescData);
}

void
orioledb_parallelscan_reinitialize(Relation rel, ParallelTableScanDesc pscan)
{
	if (!is_orioledb_rel(rel))
		ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
						errmsg("\"%s\" is not a orioledb table", NameStr(rel->rd_rel->relname))));

	orioledb_parallelscan_initialize_internal(pscan);
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
	scan = (OScanDesc) palloc0(sizeof(OScanDescData));

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
		scan->o_snapshot = o_in_progress_snapshot;
	else
		O_LOAD_SNAPSHOT(&scan->o_snapshot, snapshot);

	ItemPointerSetBlockNumber(&scan->iptr, 0);
	ItemPointerSetOffsetNumber(&scan->iptr, FirstOffsetNumber);

	if (descr)
		scan->scan = make_btree_seq_scan(&GET_PRIMARY(descr)->desc, &scan->o_snapshot, parallel_scan);

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

	scan->scan = make_btree_seq_scan(&GET_PRIMARY(descr)->desc, &scan->o_snapshot,
									 scan->rs_base.rs_parallel);
}

static void
orioledb_endscan(TableScanDesc sscan)
{
	OScanDesc	scan = (OScanDesc) sscan;

	STOPEVENT(STOPEVENT_SCAN_END, NULL);

	if (scan->rs_base.rs_flags & SO_TEMP_SNAPSHOT)
		UnregisterSnapshot(scan->rs_base.rs_snapshot);

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

	if (OidIsValid(o_saved_relrewrite))
		return false;

	do
	{
		OTuple		tuple = {0};
		BTreeLocationHint hint;
		CommitSeqNo csn;

		scan = (OScanDesc) sscan;
		descr = relation_get_descr(scan->rs_base.rs_rd);

		if (scan->scan)
			tuple = btree_seq_scan_getnext(scan->scan, slot->tts_mcxt,
										   &csn, &hint);

		if (O_TUPLE_IS_NULL(tuple))
			return false;

		tts_orioledb_store_tuple(slot, tuple, descr, csn,
								 PrimaryIndexNumber, true, &hint);

		result = slot_keytest(slot,
							  scan->rs_base.rs_nkeys,
							  scan->rs_base.rs_key);
	}
	while (!result);

	return true;
}

static void
orioledb_multi_insert(Relation relation, TupleTableSlot **slots, int ntuples,
					  CommandId cid, int options, BulkInsertState bistate)
{
	int			i;

	for (i = 0; i < ntuples; i++)
		orioledb_tuple_insert(relation, slots[i], cid, options, bistate);
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

static TransactionId
orioledb_index_delete_tuples(Relation rel, TM_IndexDeleteOp *delstate)
{
	elog(ERROR, "Not implemented");
}

void
orioledb_free_rd_amcache(Relation rel)
{
	if (rel->rd_amcache)
		table_descr_dec_refcnt((OTableDescr *) rel->rd_amcache);
	rel->rd_amcache = NULL;
}

/*
 * Comparator for sorting rows[] array
 */
static int
compare_rows(const void *a, const void *b, void *arg)
{
	HeapTuple	ha = *(const HeapTuple *) a;
	HeapTuple	hb = *(const HeapTuple *) b;
	BlockNumber ba = ItemPointerGetBlockNumber(&ha->t_self);
	OffsetNumber oa = ItemPointerGetOffsetNumber(&ha->t_self);
	BlockNumber bb = ItemPointerGetBlockNumber(&hb->t_self);
	OffsetNumber ob = ItemPointerGetOffsetNumber(&hb->t_self);

	if (ba < bb)
		return -1;
	if (ba > bb)
		return 1;
	if (oa < ob)
		return -1;
	if (oa > ob)
		return 1;
	return 0;
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
	ItemPointerData fake_iptr = {0};

	ItemPointerSetBlockNumber(&fake_iptr, 0);
	ItemPointerSetOffsetNumber(&fake_iptr, 1);

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
			ItemPointerSetBlockNumber(&slot->tts_tid, ItemPointerGetBlockNumber(&fake_iptr));
			ItemPointerSetOffsetNumber(&slot->tts_tid, ItemPointerGetOffsetNumber(&fake_iptr));
			if ((OffsetNumber) (ItemPointerGetOffsetNumber(&fake_iptr) + 1) == InvalidOffsetNumber)
			{
				ItemPointerSetBlockNumber(&fake_iptr, ItemPointerGetBlockNumber(&fake_iptr) + 1);
				ItemPointerSetOffsetNumber(&fake_iptr, 1);
			}
			else
				ItemPointerSetOffsetNumber(&fake_iptr, ItemPointerGetOffsetNumber(&fake_iptr) + 1);

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
					int			k = (int) (targrows * sampler_random_fract(&rstate.randstate));

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
	 * If we didn't find as many tuples as we wanted then we're done. No sort
	 * is needed, since they're already in order.
	 *
	 * Otherwise we need to sort the collected tuples by position
	 * (itempointer). It's not worth worrying about corner cases where the
	 * tuples are already sorted.
	 */
	if (numrows == targrows)
		qsort_interruptible(rows, numrows, sizeof(HeapTuple),
							compare_rows, NULL);

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

static void
validate_default_compress(const char *value)
{
	if (value)
		validate_compress(o_parse_compress(value), "Default");
}

static void
validate_primary_compress(const char *value)
{
	if (value)
		validate_compress(o_parse_compress(value), "Primary index");
}

static void
validate_toast_compress(const char *value)
{
	if (value)
		validate_compress(o_parse_compress(value), "TOAST");
}

/* values from StdRdOptIndexCleanup */
static relopt_enum_elt_def StdRdOptIndexCleanupValues[] =
{
	{
		"auto", STDRD_OPTION_VACUUM_INDEX_CLEANUP_AUTO
	},
	{
		"on", STDRD_OPTION_VACUUM_INDEX_CLEANUP_ON
	},
	{
		"off", STDRD_OPTION_VACUUM_INDEX_CLEANUP_OFF
	},
	{
		"true", STDRD_OPTION_VACUUM_INDEX_CLEANUP_ON
	},
	{
		"false", STDRD_OPTION_VACUUM_INDEX_CLEANUP_OFF
	},
	{
		"yes", STDRD_OPTION_VACUUM_INDEX_CLEANUP_ON
	},
	{
		"no", STDRD_OPTION_VACUUM_INDEX_CLEANUP_OFF
	},
	{
		"1", STDRD_OPTION_VACUUM_INDEX_CLEANUP_ON
	},
	{
		"0", STDRD_OPTION_VACUUM_INDEX_CLEANUP_OFF
	},
	{
		(const char *) NULL
	}							/* list terminator */
};

/*
 * Option parser for anything that uses StdRdOptions.
 */
static bytea *
orioledb_default_reloptions(Datum reloptions, bool validate, relopt_kind kind)
{
	static bool relopts_set = false;
	static local_relopts relopts = {0};

	if (!relopts_set)
	{
		MemoryContext oldcxt;

		oldcxt = MemoryContextSwitchTo(TopMemoryContext);
		init_local_reloptions(&relopts, sizeof(ORelOptions));

		/* Options from default_reloptions */
		add_local_int_reloption(&relopts, "fillfactor",
								"Packs table pages only to this percentage",
								BTREE_DEFAULT_FILLFACTOR, BTREE_MIN_FILLFACTOR,
								100,
								offsetof(ORelOptions, std_options) +
								offsetof(StdRdOptions, fillfactor));
		add_local_bool_reloption(&relopts, "autovacuum_enabled",
								 "Enables autovacuum in this relation",
								 true,
								 offsetof(ORelOptions, std_options) +
								 offsetof(StdRdOptions, autovacuum) +
								 offsetof(AutoVacOpts, enabled));
		add_local_int_reloption(&relopts, "autovacuum_vacuum_threshold",
								"Minimum number of tuple updates or deletes "
								"prior to vacuum",
								-1, 0, INT_MAX,
								offsetof(ORelOptions, std_options) +
								offsetof(StdRdOptions, autovacuum) +
								offsetof(AutoVacOpts, vacuum_threshold));
		add_local_int_reloption(&relopts, "autovacuum_vacuum_insert_threshold",
								"Minimum number of tuple inserts "
								"prior to vacuum, "
								"or -1 to disable insert vacuums",
								-2, -1, INT_MAX,
								offsetof(ORelOptions, std_options) +
								offsetof(StdRdOptions, autovacuum) +
								offsetof(AutoVacOpts,
										 vacuum_ins_threshold));
		add_local_int_reloption(&relopts, "autovacuum_analyze_threshold",
								"Minimum number of tuple inserts, "
								"updates or deletes prior to analyze",
								-1, 0, INT_MAX,
								offsetof(ORelOptions, std_options) +
								offsetof(StdRdOptions, autovacuum) +
								offsetof(AutoVacOpts, analyze_threshold));
		add_local_int_reloption(&relopts, "autovacuum_vacuum_cost_limit",
								"Vacuum cost amount available before napping, "
								"for autovacuum",
								-1, 1, 10000,
								offsetof(ORelOptions, std_options) +
								offsetof(StdRdOptions, autovacuum) +
								offsetof(AutoVacOpts, vacuum_cost_limit));
		add_local_int_reloption(&relopts, "autovacuum_freeze_min_age",
								"Minimum age at which VACUUM should freeze "
								"a table row, for autovacuum",
								-1, 0, 1000000000,
								offsetof(ORelOptions, std_options) +
								offsetof(StdRdOptions, autovacuum) +
								offsetof(AutoVacOpts, freeze_min_age));
		add_local_int_reloption(&relopts, "autovacuum_freeze_max_age",
								"Age at which to autovacuum a table "
								"to prevent transaction ID wraparound",
								-1, 100000, 2000000000,
								offsetof(ORelOptions, std_options) +
								offsetof(StdRdOptions, autovacuum) +
								offsetof(AutoVacOpts, freeze_max_age));
		add_local_int_reloption(&relopts, "autovacuum_freeze_table_age",
								"Age at which VACUUM should perform "
								"a full table sweep to freeze row versions",
								-1, 0, 2000000000,
								offsetof(ORelOptions, std_options) +
								offsetof(StdRdOptions, autovacuum) +
								offsetof(AutoVacOpts, freeze_table_age));
		add_local_int_reloption(&relopts,
								"autovacuum_multixact_freeze_min_age",
								"Minimum multixact age at which VACUUM should "
								"freeze a row multixact's, for autovacuum",
								-1, 0, 1000000000,
								offsetof(ORelOptions, std_options) +
								offsetof(StdRdOptions, autovacuum) +
								offsetof(AutoVacOpts,
										 multixact_freeze_min_age));
		add_local_int_reloption(&relopts,
								"autovacuum_multixact_freeze_max_age",
								"Multixact age at which to autovacuum a table "
								"to prevent multixact wraparound",
								-1, 10000, 2000000000,
								offsetof(ORelOptions, std_options) +
								offsetof(StdRdOptions, autovacuum) +
								offsetof(AutoVacOpts,
										 multixact_freeze_max_age));
		add_local_int_reloption(&relopts,
								"autovacuum_multixact_freeze_table_age",
								"Age of multixact at which VACUUM should "
								"perform a full table sweep to freeze "
								"row versions",
								-1, 0, 2000000000,
								offsetof(ORelOptions, std_options) +
								offsetof(StdRdOptions, autovacuum) +
								offsetof(AutoVacOpts,
										 multixact_freeze_table_age));
		add_local_int_reloption(&relopts, "log_autovacuum_min_duration",
								"Sets the minimum execution time above which "
								"autovacuum actions will be logged",
								-1, -1, INT_MAX,
								offsetof(ORelOptions, std_options) +
								offsetof(StdRdOptions, autovacuum) +
								offsetof(AutoVacOpts, log_min_duration));
		add_local_int_reloption(&relopts, "toast_tuple_target",
								"Sets the target tuple length at which "
								"external columns will be toasted",
								TOAST_TUPLE_TARGET, 128,
								TOAST_TUPLE_TARGET_MAIN,
								offsetof(ORelOptions, std_options) +
								offsetof(StdRdOptions,
										 toast_tuple_target));
		add_local_real_reloption(&relopts, "autovacuum_vacuum_cost_delay",
								 "Vacuum cost delay in milliseconds, "
								 "for autovacuum",
								 -1, 0.0, 100.0,
								 offsetof(ORelOptions, std_options) +
								 offsetof(StdRdOptions, autovacuum) +
								 offsetof(AutoVacOpts, vacuum_cost_delay));
		add_local_real_reloption(&relopts, "autovacuum_vacuum_scale_factor",
								 "Number of tuple updates or deletes prior to "
								 "vacuum as a fraction of reltuples",
								 -1, 0.0, 100.0,
								 offsetof(ORelOptions, std_options) +
								 offsetof(StdRdOptions, autovacuum) +
								 offsetof(AutoVacOpts,
										  vacuum_scale_factor));
		add_local_real_reloption(&relopts,
								 "autovacuum_vacuum_insert_scale_factor",
								 "Number of tuple inserts prior to vacuum "
								 "as a fraction of reltuples",
								 -1, 0.0, 100.0,
								 offsetof(ORelOptions, std_options) +
								 offsetof(StdRdOptions, autovacuum) +
								 offsetof(AutoVacOpts,
										  vacuum_ins_scale_factor));
		add_local_real_reloption(&relopts,
								 "autovacuum_analyze_scale_factor",
								 "Number of tuple inserts, updates or deletes "
								 "prior to analyze as a fraction of reltuples",
								 -1, 0.0, 100.0,
								 offsetof(ORelOptions, std_options) +
								 offsetof(StdRdOptions, autovacuum) +
								 offsetof(AutoVacOpts,
										  analyze_scale_factor));
		add_local_bool_reloption(&relopts, "user_catalog_table",
								 "Declare a table as an additional "
								 "catalog table, e.g. for the purpose of "
								 "logical replication",
								 false,
								 offsetof(ORelOptions, std_options) +
								 offsetof(StdRdOptions,
										  user_catalog_table));
		add_local_int_reloption(&relopts, "parallel_workers",
								"Number of parallel processes that can be "
								"used per executor node for this relation.",
								-1, 0, 1024,
								offsetof(ORelOptions, std_options) +
								offsetof(StdRdOptions, parallel_workers));
		add_local_enum_reloption(&relopts, "vacuum_index_cleanup",
								 "Controls index vacuuming and index cleanup",
								 StdRdOptIndexCleanupValues,
								 STDRD_OPTION_VACUUM_INDEX_CLEANUP_AUTO,
								 gettext_noop("Valid values are \"on\", "
											  "\"off\", and \"auto\"."),
								 offsetof(ORelOptions, std_options) +
								 offsetof(StdRdOptions,
										  vacuum_index_cleanup));
		add_local_bool_reloption(&relopts, "vacuum_truncate",
								 "Enables vacuum to truncate empty pages at "
								 "the end of this table",
								 true,
								 offsetof(ORelOptions, std_options) +
								 offsetof(StdRdOptions, vacuum_truncate));

		/* Options for orioledb tables */
		add_local_string_reloption(&relopts, "compress",
								   "Default compression level for "
								   "all table data structures",
								   NULL, validate_default_compress, NULL,
								   offsetof(ORelOptions, compress_offset));
		add_local_string_reloption(&relopts, "primary_compress",
								   "Compression level for the "
								   "table primary key",
								   NULL, validate_primary_compress, NULL,
								   offsetof(ORelOptions,
											primary_compress_offset));
		add_local_string_reloption(&relopts, "toast_compress",
								   "Compression level for the "
								   "table TOASTed values",
								   NULL, validate_toast_compress, NULL,
								   offsetof(ORelOptions,
											toast_compress_offset));
		MemoryContextSwitchTo(oldcxt);
		relopts_set = true;
	}

	return (bytea *) build_local_reloptions(&relopts, reloptions, validate);
}

static bytea *
orioledb_reloptions(char relkind, Datum reloptions, bool validate)
{
	StdRdOptions *rdopts;

	switch (relkind)
	{
		case RELKIND_TOASTVALUE:
			rdopts = (StdRdOptions *)
				default_reloptions(reloptions, validate, RELOPT_KIND_TOAST);
			if (rdopts != NULL)
			{
				/* adjust default-only parameters for TOAST relations */
				rdopts->fillfactor = 100;
				rdopts->autovacuum.analyze_threshold = -1;
				rdopts->autovacuum.analyze_scale_factor = -1;
			}
			return (bytea *) rdopts;
		case RELKIND_RELATION:
		case RELKIND_MATVIEW:
			return orioledb_default_reloptions(reloptions, validate,
											   RELOPT_KIND_HEAP);
		default:
			/* other relkinds are not supported */
			return NULL;
	}
}

static bool
orioledb_tuple_is_current(Relation rel, TupleTableSlot *slot)
{
	OTableSlot *oslot = (OTableSlot *) slot;

	return COMMITSEQNO_IS_INPROGRESS(oslot->csn);
}


/* ------------------------------------------------------------------------
 * Definition of the orioledb table access method.
 * ------------------------------------------------------------------------
 */

static const TableAmRoutine orioledb_am_methods = {
	.type = T_TableAmRoutine,
	.slot_callbacks = orioledb_slot_callbacks,
	.get_row_ref_type = orioledb_get_row_ref_type,
	.free_rd_amcache = orioledb_free_rd_amcache,

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
	.index_delete_tuples = orioledb_index_delete_tuples,

	.tuple_insert = orioledb_tuple_insert,
	.tuple_insert_with_arbiter = orioledb_tuple_insert_with_arbiter,
	.multi_insert = orioledb_multi_insert,
	.tuple_delete = orioledb_tuple_delete,
	.tuple_update = orioledb_tuple_update,
	.tuple_lock = orioledb_tuple_lock,
	.finish_bulk_insert = orioledb_finish_bulk_insert,

	.tuple_fetch_row_version = orioledb_fetch_row_version,
	.tuple_get_latest_tid = orioledb_get_latest_tid,
	.tuple_tid_valid = orioledb_tuple_tid_valid,
	.tuple_satisfies_snapshot = orioledb_tuple_satisfies_snapshot,

#if PG_VERSION_NUM >= 160000
	.relation_set_new_filelocator = orioledb_relation_set_new_filenode,
#else
	.relation_set_new_filenode = orioledb_relation_set_new_filenode,
#endif
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
	.scan_sample_next_tuple = orioledb_scan_sample_next_tuple,
	.tuple_is_current = orioledb_tuple_is_current,
	.analyze_table = orioledb_analyze_table,
	.reloptions = orioledb_reloptions
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
	ORelOids	oids;

	ORelOidsSetFromRel(oids, rel);
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

static void
rowid_set_csn(OIndexDescr *id, Datum pkDatum, CommitSeqNo csn)
{
	bytea	   *rowid;
	Pointer		p;

	if (!id->primaryIsCtid)
	{
		ORowIdAddendumNonCtid *add;

		rowid = DatumGetByteaP(pkDatum);
		p = (Pointer) rowid + MAXALIGN(VARHDRSZ);
		add = (ORowIdAddendumNonCtid *) p;
		add->csn = csn;
	}
	else
	{
		ORowIdAddendumCtid *add;

		rowid = DatumGetByteaP(pkDatum);
		p = (Pointer) rowid + MAXALIGN(VARHDRSZ);
		add = (ORowIdAddendumCtid *) p;
		add->csn = csn;
	}
}
