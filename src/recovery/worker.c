/*-------------------------------------------------------------------------
 *
 * worker.c
 *		Recovery worker process implementation for orioledb engine.
 *
 * Copyright (c) 2021-2023, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/recovery/worker.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "orioledb.h"

#include "btree/modify.h"
#include "catalog/indices.h"
#include "catalog/o_sys_cache.h"
#include "catalog/o_tables.h"
#include "recovery/recovery.h"
#include "recovery/internal.h"
#include "tableam/descr.h"
#include "tableam/operations.h"
#include "tableam/tree.h"
#include "transam/oxid.h"
#include "tuple/slot.h"

#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/pmsignal.h"
#include "storage/proc.h"
#include "storage/shm_mq.h"
#include "storage/sinvaladt.h"
#include "utils/inval.h"
#include "utils/syscache.h"
#include "utils/timeout.h"

#if PG_VERSION_NUM >= 140000
#include "utils/wait_event.h"
#endif

#define QUEUE_READ_USLEEP_BASE		(10)
#define QUEUE_READ_USLEEP_MULTIPLER	(2)
#define QUEUE_READ_USLEEP_MAX		(1024 * QUEUE_READ_USLEEP_BASE)

static bool detached = false;
static CommitSeqNo my_ptr;

static void recovery_queue_process(shm_mq_handle *queue, int id);
static inline Pointer recovery_queue_read(shm_mq_handle *queue, Size *data_size, int id);
static void apply_tbl_modify_record(OTableDescr *descr, uint16 type,
									OTuple p, OXid oxid, CommitSeqNo csn);
static void apply_tbl_insert(OTableDescr *descr, OTuple tuple,
							 OXid oxid, CommitSeqNo csn);
static void apply_tbl_delete(OTableDescr *descr, OTuple key,
							 OXid oxid, CommitSeqNo csn);
static void apply_tbl_update(OTableDescr *descr, OTuple tuple,
							 OXid oxid, CommitSeqNo csn);

typedef struct
{
	OTuple		tuple;
	OTuple		key;
	CommitSeqNo csn;
} CallbackTupleCopy;

/*
 * Callback examples which stores modified tuple as arg.
 */
static OBTreeModifyCallbackAction
o_delete_copy_callback(BTreeDescr *descr,
					   OTuple tup, OTuple *newtup, OXid oxid,
					   OTupleXactInfo xactInfo, UndoLocation location,
					   RowLockMode *lock_mode, BTreeLocationHint *hint, void *arg)
{
	CallbackTupleCopy *copyArg = (CallbackTupleCopy *) arg;

	if (descr->type == oIndexPrimary || descr->type == oIndexToast)
	{
		if (XACT_INFO_OXID_EQ(xactInfo, oxid) &&
			o_tuple_get_version(tup) > o_tuple_get_version(copyArg->key))
			return OBTreeCallbackActionUndo;
	}

	if (descr->type == oIndexPrimary)
	{
		OIndexDescr *id = (OIndexDescr *) descr->arg;
		Size		sz = o_tuple_size(tup, &id->leafSpec);

		copyArg->tuple.data = (Pointer) palloc(sz);
		copyArg->tuple.formatFlags = tup.formatFlags;
		memcpy(copyArg->tuple.data, tup.data, sz);
		if (XACT_INFO_IS_FINISHED(xactInfo))
			copyArg->csn = COMMITSEQNO_MAX_NORMAL;
		else
			copyArg->csn = COMMITSEQNO_INPROGRESS;
	}
	return OBTreeCallbackActionDelete;
}

static OBTreeModifyCallbackAction
o_update_copy_callback(BTreeDescr *descr,
					   OTuple tup, OTuple *newtup, OXid oxid,
					   OTupleXactInfo xactInfo, UndoLocation location,
					   RowLockMode *lock_mode, BTreeLocationHint *hint, void *arg)
{
	CallbackTupleCopy *copyArg = (CallbackTupleCopy *) arg;

	if (descr->type == oIndexPrimary || descr->type == oIndexToast)
	{
		if (XACT_INFO_OXID_EQ(xactInfo, oxid) &&
			o_tuple_get_version(tup) >= o_tuple_get_version(*newtup))
			return OBTreeCallbackActionUndo;
	}

	if (descr->type == oIndexPrimary)
	{
		OIndexDescr *id = (OIndexDescr *) descr->arg;
		Size		sz = o_tuple_size(tup, &id->leafSpec);

		copyArg->tuple.data = (Pointer) palloc(sz);
		copyArg->tuple.formatFlags = tup.formatFlags;
		memcpy(copyArg->tuple.data, tup.data, sz);
		if (XACT_INFO_IS_FINISHED(xactInfo))
			copyArg->csn = COMMITSEQNO_MAX_NORMAL;
		else
			copyArg->csn = COMMITSEQNO_INPROGRESS;
	}
	return OBTreeCallbackActionUpdate;
}

/*
 * Registers a new recovery worker. Returns NULL typically if no background
 * workers slots available.
 */
BackgroundWorkerHandle *
recovery_worker_register(int worker_id)
{
	char		worker_name[128];
	BackgroundWorker worker;
	BackgroundWorkerHandle *handle = NULL;

	sprintf(worker_name, "orioledb recovery worker %d", worker_id);
	/* Set up background worker parameters */
	memset(&worker, 0, sizeof(worker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS;
	worker.bgw_start_time = BgWorkerStart_PostmasterStart;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	worker.bgw_main_arg = worker_id;
	strcpy(worker.bgw_library_name, "orioledb");
	strcpy(worker.bgw_function_name, "recovery_worker_main");
	strcpy(worker.bgw_name, worker_name);
	strcpy(worker.bgw_type, "orioledb recovery worker");

	if (MyProcPid == PostmasterPid)
	{
		RegisterBackgroundWorker(&worker);
	}
	else
	{
		worker.bgw_notify_pid = MyProcPid;
		RegisterDynamicBackgroundWorker(&worker, &handle);
	}

	return handle;
}

static void
handle_sigterm(SIGNAL_ARGS)
{
	detached = true;
	SetLatch(MyLatch);
}

/*
 * Recovery worker main function.
 */
void
recovery_worker_main(Datum main_arg)
{
	shm_mq_handle *recovery_worker_queue = NULL;

	PG_TRY();
	{
		int			id = main_arg;

		elog(LOG, "orioledb recovery worker %d started.", id);

		/* enable timeout for relation lock */
		RegisterTimeout(DEADLOCK_TIMEOUT, CheckDeadLockAlert);
		/* enable relation cache invalidation (remove old table descriptors) */
		RelationCacheInitialize();
		SharedInvalBackendInit(false);

		SetProcessingMode(NormalProcessing);

		ResetLatch(MyLatch);

		pqsignal(SIGTERM, handle_sigterm);
		BackgroundWorkerUnblockSignals();

		before_shmem_exit(recovery_on_proc_exit, main_arg);
		recovery_init(id);

		shm_mq_set_receiver(GET_WORKER_QUEUE(id), MyProc);
		recovery_worker_queue = shm_mq_attach(GET_WORKER_QUEUE(id), NULL, NULL);

		my_ptr = pg_atomic_read_u64(&worker_ptrs[id].commitPtr);
		recovery_queue_process(recovery_worker_queue, id);
		if (detached)
		{
			elog(ERROR, "orioledb recovery worker %d finished: unexpected detach from recovery messages queue.", id);
		}

		shm_mq_detach(recovery_worker_queue);
		recovery_worker_queue = NULL;

		recovery_finish(id);
		LockReleaseSession(DEFAULT_LOCKMETHOD);

		if (id <= index_build_leader)
			pg_atomic_fetch_add_u32(worker_finish_count, 1);
		else
			pg_atomic_fetch_add_u32(idx_worker_finish_count, 1);

		elog(LOG, "orioledb recovery worker %d finished.", id);
		proc_exit(0);
	}
	PG_CATCH();
	{
		if (recovery_worker_queue != NULL)
		{
			/* detach from queue if attached */
			shm_mq_detach(recovery_worker_queue);
		}

		/*
		 * Don't call recovery_finish().  We haven't receive the finish
		 * message from main recovery process.  So, we aren't promoted.
		 * Information about running transactions might be needed by
		 * checkpoint.
		 */
		LockReleaseSession(DEFAULT_LOCKMETHOD);
		PG_RE_THROW();
	}
	PG_END_TRY();
}

static inline void
update_worker_ptr(int worker_id, XLogRecPtr ptr)
{
	pg_atomic_add_fetch_u32(worker_ptrs_changes, 1);

	pg_atomic_write_u64(&worker_ptrs[worker_id].commitPtr, ptr);

	pg_atomic_add_fetch_u32(worker_ptrs_changes, 1);
	my_ptr = ptr;
}

/*
 * Reads messages from recovery queue and applies modify records to BTrees.
 */
static void
recovery_queue_process(shm_mq_handle *queue, int id)
{
	RecoveryMsgOXidPtr *oxid_csn_record;
	RecoveryMsgPtr *csn_record;
	RecoveryMsgHeader *recovery_header;
	OTableDescr *descr = NULL;
	OIndexDescr *indexDescr = NULL;
	Pointer		data;
	OIndexType	ix_type;
	ORelOids	oids = {InvalidOid, InvalidOid, InvalidOid};
	int			tuple_len;
	Size		data_size,
				data_pos;
	bool		finished = false;
	OXid		oxid;

	while (!finished)
	{
		data = recovery_queue_read(queue, &data_size, id);
		if (detached)
			break;

		Assert(data != NULL);
		data_pos = 0;
		while (data_pos < data_size)
		{
			recovery_header = (RecoveryMsgHeader *) (data + data_pos);
			if (recovery_header->type & RECOVERY_MODIFY)
			{
				OTuple		tuple;

				data_pos += sizeof(RecoveryMsgHeader);
				if (recovery_header->type & RECOVERY_MODIFY_OXID)
				{
					memcpy(&oxid, data + data_pos, sizeof(OXid));
					data_pos += sizeof(OXid);
					recovery_switch_to_oxid(oxid, id);
				}

				if (recovery_header->type & RECOVERY_MODIFY_OIDS)
				{
					memcpy(&oids, data + data_pos, sizeof(ORelOids));
					data_pos += sizeof(ORelOids);
					ix_type = *(data + data_pos);
					data_pos++;
					if (descr)
					{
						table_descr_dec_refcnt(descr);
						descr = NULL;
					}
					AcceptInvalidationMessages();
					if (ix_type == oIndexInvalid)
					{
						descr = o_fetch_table_descr(oids);
						table_descr_inc_refcnt(descr);
						indexDescr = GET_PRIMARY(descr);
					}
					else
					{
						indexDescr = o_fetch_index_descr(oids, ix_type,
														 false,
														 NULL);
					}
				}

				memcpy(&tuple_len, data + data_pos, sizeof(int));
				data_pos += sizeof(int);
				memcpy(&tuple.formatFlags, data + data_pos, 1);
				data_pos++;
				data_pos = MAXALIGN(data_pos);
				if (indexDescr != NULL)
				{
					Assert(ORelOidsIsValid(oids));

					tuple.data = data + data_pos;
					apply_modify_record(descr, indexDescr,
										(recovery_header->type & RECOVERY_MODIFY),
										tuple);
				}
				data_pos += tuple_len;
			}
#if PG_VERSION_NUM >= 140000
			else if (recovery_header->type & (RECOVERY_LEADER_PARALLEL_INDEX_BUILD | RECOVERY_WORKER_PARALLEL_INDEX_BUILD))
			{
				RecoveryOidsMsgIdxBuild *msg = (RecoveryOidsMsgIdxBuild *) (data + data_pos);
				OTable	   *o_table;

				Assert(ORelOidsIsValid(msg->oids));
				recovery_oxid = recovery_oidxshared->recovery_oxid;
				o_table = o_tables_get_by_oids_and_version(msg->oids, &msg->o_table_version);
				Assert(o_table);
				Assert(o_table->version == msg->o_table_version);

				if (recovery_header->type & RECOVERY_LEADER_PARALLEL_INDEX_BUILD)
				{
					OTableDescr *o_descr = (OTableDescr *) palloc0(sizeof(OTableDescr));

					Assert(id == index_build_leader);
					o_fill_tmp_table_descr(o_descr, o_table);

					build_secondary_index(o_table, o_descr, msg->ix_num, true);

					/*
					 * Wake up the other recovery processes that may wait to
					 * do their modify operations on this relation or to do
					 * oxid update
					 */
					SpinLockAcquire(&recovery_oidxshared->mutex);
					recovery_oidxshared->completed_position = msg->current_position;
					SpinLockRelease(&recovery_oidxshared->mutex);
					ConditionVariableBroadcast(&recovery_oidxshared->recoverycv);
					o_free_tmp_table_descr(o_descr);
					pfree(o_descr);
				}
				else if (recovery_header->type & RECOVERY_WORKER_PARALLEL_INDEX_BUILD)
				{
					Assert(id >= index_build_first_worker && id <= index_build_last_worker);
					/* participate as a worker in parallel index build */
					_o_index_parallel_build_inner(NULL, NULL, o_table);
				}

				data_pos += sizeof(RecoveryOidsMsgIdxBuild);
				pfree(o_table);
			}
#endif
			else if (recovery_header->type & RECOVERY_COMMIT)
			{
				oxid_csn_record = (RecoveryMsgOXidPtr *) (data + data_pos);
				recovery_switch_to_oxid(oxid_csn_record->oxid, id);
				recovery_finish_current_oxid(COMMITSEQNO_MAX_NORMAL - 1,
											 oxid_csn_record->ptr,
											 id,
											 false);
				update_worker_ptr(id, oxid_csn_record->ptr);
				data_pos += sizeof(RecoveryMsgOXidPtr);
			}
			else if (recovery_header->type & RECOVERY_ROLLBACK)
			{
				oxid_csn_record = (RecoveryMsgOXidPtr *) (data + data_pos);
				recovery_switch_to_oxid(oxid_csn_record->oxid, id);
				recovery_finish_current_oxid(COMMITSEQNO_ABORTED,
											 oxid_csn_record->ptr,
											 id,
											 false);
				update_worker_ptr(id, oxid_csn_record->ptr);
				data_pos += sizeof(RecoveryMsgOXidPtr);
			}
			else if (recovery_header->type & RECOVERY_SYNCHRONIZE)
			{
				csn_record = (RecoveryMsgPtr *) (data + data_pos);
				update_worker_ptr(id, csn_record->ptr);
				data_pos += sizeof(RecoveryMsgPtr);
			}
			else if (recovery_header->type & RECOVERY_FINISHED)
			{
#if PG_VERSION_NUM >= 140000
				if (id == index_build_leader)
					idx_workers_shutdown();
#endif
				finished = true;
				break;
			}
			else if (recovery_header->type & RECOVERY_TOAST_CONSISTENT)
			{
				toast_consistent = true;
				data_pos += sizeof(RecoveryMsgEmpty);
			}
			else if (recovery_header->type & RECOVERY_SAVEPOINT)
			{
				RecoveryMsgSavepoint *msg;

				msg = (RecoveryMsgSavepoint *) (data + data_pos);
				recovery_switch_to_oxid(msg->oxid, id);
				recovery_savepoint(msg->parentSubId, id);
				data_pos += sizeof(RecoveryMsgSavepoint);
			}
			else if (recovery_header->type & RECOVERY_ROLLBACK_TO_SAVEPOINT)
			{
				RecoveryMsgRollbackToSavepoint *msg;

				msg = (RecoveryMsgRollbackToSavepoint *) (data + data_pos);
				recovery_switch_to_oxid(msg->oxid, id);
				recovery_rollback_to_savepoint(msg->parentSubId, id);
				update_worker_ptr(id, msg->ptr);
				data_pos += sizeof(RecoveryMsgRollbackToSavepoint);
			}
			else
			{
				Assert(false);
			}
			data_pos = MAXALIGN(data_pos);
		}
		update_recovery_undo_loc_flush(false, id);
	}
	if (descr)
		table_descr_dec_refcnt(descr);
}

/*
 * Apply the modify WAL record.
 */
void
apply_modify_record(OTableDescr *descr, OIndexDescr *id, uint16 type,
					OTuple p)
{
	OXid		oxid;
	CommitSeqNo csn;

	oxid = get_current_oxid();
	csn = COMMITSEQNO_INPROGRESS;

	/*
	 * Don't apply changes to secondary indices before TOAST is consisntent.
	 * Otherwise, values of secondary indices on TOASTed fields can be
	 * invalid.
	 */
	if (descr && toast_consistent)
	{
		/* Modify table */
		apply_tbl_modify_record(descr, type, p, oxid, csn);
	}
	else
	{
		o_btree_load_shmem(&id->desc);
		apply_btree_modify_record(&id->desc, type, p, oxid, csn);
	}
}

/*
 * Reads a message from the queue.
 */
static inline Pointer
recovery_queue_read(shm_mq_handle *queue, Size *data_size, int id)
{
	shm_mq_result read_result;
	XLogRecPtr	prev_rec_ptr = InvalidXLogRecPtr,
				cur_rec_ptr;
	long		usleep_time;
	Pointer		data = NULL;

	usleep_time = QUEUE_READ_USLEEP_BASE;
	while (true)
	{
		read_result = shm_mq_receive(queue, data_size, (void **) &data, true);

		if (read_result == SHM_MQ_SUCCESS)
		{
			break;
		}
		else if (read_result == SHM_MQ_DETACHED || detached)
		{
			detached = true;
			data = NULL;
			break;
		}

		/*
		 * else the queue is empty
		 */
		Assert(read_result == SHM_MQ_WOULD_BLOCK);

		/* we can try to update our ptr if queue is empty */
		cur_rec_ptr = pg_atomic_read_u64(recovery_ptr);
		if (cur_rec_ptr > my_ptr)
		{
			if (!XLogRecPtrIsInvalid(prev_rec_ptr))
			{
				update_worker_ptr(id, prev_rec_ptr);
			}

			/*
			 * if cur_skiped is a new value than we need to recheck is queue
			 * still empty before apply it
			 */
			prev_rec_ptr = cur_rec_ptr != prev_rec_ptr ? cur_rec_ptr : InvalidXLogRecPtr;
			continue;
		}

		prev_rec_ptr = InvalidXLogRecPtr;

		pg_usleep(usleep_time);
		update_proc_retain_undo_location(id);
		update_recovery_undo_loc_flush(false, id);

		if (!PostmasterIsAlive() || detached)
		{
			detached = true;
			data = NULL;
			break;
		}

		if (usleep_time != QUEUE_READ_USLEEP_MAX)
			usleep_time *= QUEUE_READ_USLEEP_MULTIPLER;
	}

	return data;
}

/*
 * Applies table modify records. We skip unique indices because recovery master
 * distributes it separately.
 */
static void
apply_tbl_modify_record(OTableDescr *descr, uint16 type,
						OTuple p, OXid oxid, CommitSeqNo csn)
{
	o_set_syscache_hooks();
	switch (type)
	{
		case RECOVERY_INSERT:
			apply_tbl_insert(descr, p, oxid, csn);
			return;
		case RECOVERY_DELETE:
			apply_tbl_delete(descr, p, oxid, csn);
			return;
		case RECOVERY_UPDATE:
			apply_tbl_update(descr, p, oxid, csn);
			return;
		default:
			Assert(false);
			elog(ERROR, "Wrong primary index modify record type %d", type);
	}
	o_unset_syscache_hooks();
}

static void
apply_tbl_insert(OTableDescr *descr, OTuple tuple,
				 OXid oxid, CommitSeqNo csn)
{
	OBTreeKeyBound keyBound;
	OTuple		stuple,
				cur_tuple;
	OIndexDescr *id;
	int			i;
	bool		isPrimary = false;
	TupleTableSlot *slot = descr->newTuple;
	BTreeModifyCallbackInfo callbackInfo = nullCallbackInfo;

	O_TUPLE_SET_NULL(stuple);

	tts_orioledb_store_tuple(slot, tuple, descr,
							 csn, PrimaryIndexNumber, false, NULL);

	if (GET_PRIMARY(descr)->primaryIsCtid)
	{
		o_btree_load_shmem(&GET_PRIMARY(descr)->desc);
		btree_ctid_update_if_needed(&GET_PRIMARY(descr)->desc,
									slot->tts_tid);
	}

	for (i = 0; i < descr->nIndices; i++)
	{
		isPrimary = (i == PrimaryIndexNumber);
		id = descr->indices[i];

		if (!isPrimary)
		{
			stuple = tts_orioledb_make_secondary_tuple(slot, descr->indices[i], true);
		}

		if (!isPrimary)
		{
			if (!o_is_index_predicate_satisfied(id, slot, id->econtext))
				continue;
		}

		cur_tuple = isPrimary ? tuple : stuple;

		o_btree_load_shmem(&id->desc);

		if (isPrimary)
		{
			callbackInfo.modifyCallback = recovery_insert_primary_callback;
			callbackInfo.modifyDeletedCallback = recovery_insert_deleted_primary_callback;
		}
		else
		{
			callbackInfo.modifyCallback = recovery_insert_overwrite_callback;
			callbackInfo.modifyDeletedCallback = recovery_insert_deleted_overwrite_callback;
		}
		tts_orioledb_fill_key_bound(slot, id, &keyBound);
		/* HACK: prevent sys cache pages from loading during o_btree_modify */
		(void) o_btree_cmp(&id->desc, &cur_tuple, BTreeKeyLeafTuple,
						   (Pointer) &keyBound, BTreeKeyBound);
		(void) o_btree_modify(&id->desc, BTreeOperationInsert,
							  cur_tuple, BTreeKeyLeafTuple,
							  (Pointer) &keyBound, BTreeKeyBound,
							  oxid, csn, RowLockUpdate,
							  NULL, &callbackInfo);

		if (!isPrimary)
		{
			pfree(stuple.data);
			O_TUPLE_SET_NULL(stuple);
		}
	}

	if (!O_TUPLE_IS_NULL(stuple))
		pfree(stuple.data);
	ExecClearTuple(slot);
}

static void
apply_tbl_delete(OTableDescr *descr, OTuple key,
				 OXid oxid, CommitSeqNo csn)
{
	OBTreeModifyResult modify_result;
	OBTreeKeyBound keyBound;
	CallbackTupleCopy tupCopy;
	OIndexDescr *id;
	int			i;
	bool		isPrimary;
	TupleTableSlot *slot = descr->newTuple;
	OTuple		nullTup;

	O_TUPLE_SET_NULL(nullTup);
	for (i = 0; i < descr->nIndices; i++)
	{
		isPrimary = i == PrimaryIndexNumber;
		id = descr->indices[i];

		o_btree_load_shmem(&id->desc);
		if (isPrimary)
		{
			BTreeModifyCallbackInfo callbackInfo = {
				.waitCallback = NULL,
				.modifyCallback = o_delete_copy_callback,
				.modifyDeletedCallback = NULL,
				.needsUndoForSelfCreated = false,
				.arg = &tupCopy
			};

			o_fill_key_bound(id, key, BTreeKeyNonLeafKey, &keyBound);
			O_TUPLE_SET_NULL(tupCopy.tuple);
			tupCopy.key = key;
			modify_result = o_btree_modify(&id->desc, BTreeOperationDelete,
										   nullTup, BTreeKeyNone,
										   (Pointer) &keyBound, BTreeKeyBound,
										   oxid, csn, RowLockUpdate,
										   NULL, &callbackInfo);
			if (modify_result != OBTreeModifyResultDeleted)
				return;

			if (descr->nIndices == 1)
			{
				pfree(tupCopy.tuple.data);
				return;
			}

			tts_orioledb_store_tuple(slot, tupCopy.tuple, descr,
									 tupCopy.csn, PrimaryIndexNumber, true, NULL);
		}
		else
		{
			BTreeModifyCallbackInfo callbackInfo = {
				.waitCallback = NULL,
				.modifyCallback = recovery_delete_overwrite_callback,
				.modifyDeletedCallback = NULL,
				.needsUndoForSelfCreated = false,
				.arg = NULL
			};

			Assert(!TTS_EMPTY(slot));
			if (!o_is_index_predicate_satisfied(id, slot, id->econtext))
				continue;
			tts_orioledb_fill_key_bound(slot, id, &keyBound);
			o_btree_modify(&id->desc, BTreeOperationDelete,
						   nullTup, BTreeKeyNone,
						   (Pointer) &keyBound, BTreeKeyBound,
						   oxid, csn, RowLockUpdate,
						   NULL, &callbackInfo);
		}
	}

	ExecClearTuple(slot);
}

static void
apply_tbl_update(OTableDescr *descr, OTuple tuple,
				 OXid oxid, CommitSeqNo csn)
{
	OBTreeModifyResult modify_result;
	OBTreeKeyBound old_key,
				new_key;
	OTuple		new_stup;
	CallbackTupleCopy tupCopy;
	OIndexDescr *tree;
	int			i;
	bool		isPrimary;
	TupleTableSlot *new_slot = descr->newTuple,
			   *old_slot = descr->oldTuple;

	for (i = 0; i < descr->nIndices; i++)
	{
		isPrimary = i == PrimaryIndexNumber;
		tree = descr->indices[i];

		o_btree_load_shmem(&tree->desc);
		if (isPrimary)
		{
			BTreeModifyCallbackInfo callbackInfo = {
				.waitCallback = NULL,
				.modifyCallback = o_update_copy_callback,
				.modifyDeletedCallback = NULL,
				.needsUndoForSelfCreated = false,
				.arg = &tupCopy
			};

			O_TUPLE_SET_NULL(tupCopy.tuple);
			modify_result = o_btree_modify(&tree->desc, BTreeOperationUpdate,
										   tuple, BTreeKeyLeafTuple,
										   NULL, BTreeKeyNone, oxid, csn,
										   RowLockNoKeyUpdate,
										   NULL, &callbackInfo);
			if (modify_result != OBTreeModifyResultUpdated)
				return;

			if (descr->nIndices == 1)
			{
				pfree(tupCopy.tuple.data);
				return;
			}

			tts_orioledb_store_tuple(new_slot, tuple, descr,
									 csn, PrimaryIndexNumber, false, NULL);
			tts_orioledb_store_tuple(old_slot, tupCopy.tuple, descr,
									 tupCopy.csn, PrimaryIndexNumber, true, NULL);
		}
		else
		{
			int			cmp;

			tts_orioledb_fill_key_bound(new_slot, tree, &new_key);
			tts_orioledb_fill_key_bound(old_slot, tree, &old_key);

			cmp = o_btree_cmp(&tree->desc,
							  (Pointer) &new_key, BTreeKeyBound,
							  (Pointer) &old_key, BTreeKeyBound);

			if (cmp != 0)
			{
				OTuple		nullTup;
				BTreeModifyCallbackInfo callbackInfo = nullCallbackInfo;

				O_TUPLE_SET_NULL(nullTup);

				if (o_is_index_predicate_satisfied(tree,
												   old_slot,
												   tree->econtext))
				{
					callbackInfo.modifyCallback = recovery_delete_overwrite_callback;
					(void) o_btree_modify(&tree->desc, BTreeOperationDelete,
										  nullTup, BTreeKeyNone,
										  (Pointer) &old_key, BTreeKeyBound,
										  oxid, csn, RowLockUpdate,
										  NULL, &callbackInfo);
				}

				if (o_is_index_predicate_satisfied(tree,
												   new_slot,
												   tree->econtext))
				{
					callbackInfo.modifyDeletedCallback = recovery_insert_deleted_overwrite_callback;
					callbackInfo.modifyCallback = recovery_insert_overwrite_callback;
					new_stup = tts_orioledb_make_secondary_tuple(new_slot, tree, true);
					(void) o_btree_modify(&tree->desc, BTreeOperationInsert,
										  new_stup, BTreeKeyLeafTuple,
										  (Pointer) &new_key, BTreeKeyBound,
										  oxid, csn, RowLockUpdate,
										  NULL, &callbackInfo);
					pfree(new_stup.data);
				}
			}
		}
	}

	ExecClearTuple(new_slot);
	ExecClearTuple(old_slot);
}
