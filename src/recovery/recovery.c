/*-------------------------------------------------------------------------
 *
 * recovery.c
 *		General routines for orioledb recovery.
 *
 * Copyright (c) 2021-2025, Oriole DB Inc.
 * Copyright (c) 2025, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/recovery/recovery.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "orioledb.h"

#include "btree/btree.h"
#include "btree/io.h"
#include "btree/modify.h"
#include "btree/page_chunks.h"
#include "btree/undo.h"
#include "catalog/free_extents.h"
#include "catalog/indices.h"
#include "catalog/o_sys_cache.h"
#include "checkpoint/checkpoint.h"
#include "recovery/recovery.h"
#include "recovery/internal.h"
#include "recovery/wal.h"
#include "storage/itemptr.h"
#include "tableam/descr.h"
#include "tableam/operations.h"
#include "tableam/tree.h"
#include "transam/oxid.h"
#include "transam/undo.h"
#include "utils/dsa.h"
#include "utils/inval.h"
#include "utils/page_pool.h"
#include "utils/stopevent.h"
#include "utils/syscache.h"
#include "workers/interrupt.h"

#include "access/hash.h"
#include "access/xlog_internal.h"
#include "access/xlogrecovery.h"
#include "lib/ilist.h"
#include "lib/pairingheap.h"
#include "miscadmin.h"
#include "postmaster/postmaster.h"
#include "postmaster/startup.h"
#include "pgstat.h"
#include "replication/message.h"
#include "storage/ipc.h"
#include "storage/shm_mq.h"
#include "storage/standby.h"
#include "utils/memdebug.h"
#include "utils/memutils.h"
#include "utils/typcache.h"

#include <sys/stat.h>
#include <unistd.h>

/*
 * Recovery worker state in pool.
 */
typedef struct
{
	/* Pointer to the worker queue */
	shm_mq_handle *queue;
	char		queue_buf[RECOVERY_QUEUE_BUF_SIZE];
	int			queue_buf_len;
	/* Current oids */
	ORelOids	oids;
	/* Current oxid */
	OXid		oxid;
	/* Current index type */
	OIndexType	type;
	/* Handle for the worker */
	BackgroundWorkerHandle *handle;
} RecoveryWorkerState;

static RecoveryWorkerState *workers_pool;

typedef struct
{
	ORelOids	oids;			/* hash table key */
	uint64		position;
} RecoveryIdxBuildQueueState;

/*
 * Recovery transaction state.
 */
typedef struct
{
	OXid		oxid;			/* hash table key */

	TransactionId xid;			/* builtin transaction identifier for joint
								 * commit */

	bool		needs_wal_flush;
	UndoLocation retain_locs[(int) UndoLogsCount];
	UndoStackLocations undo_stacks[(int) UndoLogsCount];
	dlist_head	checkpoint_undo_stacks;
	CommitSeqNo csn;
	XLogRecPtr	ptr;

	bool		in_finished_list;
	bool		in_retain_undo_heaps[(int) UndoLogsCount];

	dlist_node	joint_commit_list_node;
	dlist_node	finished_list_node;
	pairingheap_node retain_undo_ph_nodes[(int) UndoLogsCount];
	pairingheap_node xmin_ph_node;

	/* is any system tree modified by oxid */
	bool		systree_modified;
	/* is typecache invalidation needed after this transaction */
	bool		invalidate_typcache;
	/* is oTablesMetaLock held by transaction */
	bool		o_tables_meta_locked;
	/* is provided by checkpoint xids file */
	bool		checkpoint_xid;
	/* is started from wal stream */
	bool		wal_xid;
	/* usage map */
	bool	   *used_by;
} RecoveryXidState;

#define RetainUndoNodeGetRecoveryXidState(node, undoType) \
	((RecoveryXidState *) ((Pointer) (node) - \
		offsetof(RecoveryXidState, retain_undo_ph_nodes) - \
		sizeof(pairingheap_node) * (int) (undoType)))

typedef struct
{
	UndoLogType undoType;
	UndoStackLocations undoStack;
	dlist_node	node;
} CheckpointUndoStack;


PG_FUNCTION_INFO_V1(orioledb_recovery_synchronized);

/*
 * Comparator for undo retain min-heap.
 *
 * See pairingheap.c/pairingheap_comparator description.
 */
static int
retain_undo_pairingheap_cmp(const pairingheap_node *a,
							const pairingheap_node *b,
							void *arg)
{
	int			num = *((int *) arg);
	const RecoveryXidState *l = RetainUndoNodeGetRecoveryXidState(a, num);
	const RecoveryXidState *r = RetainUndoNodeGetRecoveryXidState(b, num);;

	if (l->retain_locs[UndoLogRegular] < r->retain_locs[UndoLogRegular])
		return 1;
	else if (l->retain_locs[UndoLogRegular] > r->retain_locs[UndoLogRegular])
		return -1;
	else
		return 0;
}

/*
 * Comparator for xmin min-heap.
 *
 * See pairingheap.c/pairingheap_comparator description.
 */
static int
xmin_pairingheap_cmp(const pairingheap_node *a,
					 const pairingheap_node *b,
					 void *arg)
{
	const RecoveryXidState *l = pairingheap_const_container(RecoveryXidState, xmin_ph_node, a);
	const RecoveryXidState *r = pairingheap_const_container(RecoveryXidState, xmin_ph_node, b);

	if (l->oxid < r->oxid)
		return 1;
	else if (l->oxid > r->oxid)
		return -1;
	else
		return 0;
}

/* Current recovery transaction state. */
static RecoveryXidState *cur_state = NULL;

/* Recovery transaction hash for the current process. */
static HTAB *recovery_xid_state_hash = NULL;

static HTAB *idxbuild_oids_hash = NULL;

/* Queues of undo retain locations */
static pairingheap *retain_undo_queues[(int) UndoLogsCount] =
{
	NULL
};
static int	retain_undo_queue_numbers[(int) UndoLogsCount];

/* Queue of xmin's */
static pairingheap *xmin_queue = NULL;

/*
 * List of locally finished transaction, which aren't yet knows as finished
 * for every recovery process.
 */
static dlist_head finished_list;

/*
 * List of transactions waiting for joint commit with builtin transaction.
 */
static dlist_head joint_commit_list;

/* orioledb checkpoint number from which we start recovery */
static uint32 startup_chkp_num;

/* is recovery main process has error */
static bool unexpected_worker_detach = false;

/*
 * True if current process is a recovery process (worker or master).
 */
static bool iam_recovery = false;

/*
 * Current orioledb transaction recovery id
 */
OXid		recovery_oxid = InvalidOXid;

/*
 * Full size of a recovery queue.
 */
uint64		recovery_queue_data_size = 0;

/*
 * The pointer to a first recovery queue.
 */
Pointer		recovery_first_queue = NULL;

/*
 * GUC value, number of recovery workers.
 */
int			recovery_pool_size_guc;
int			recovery_idx_pool_size_guc;

/*
 * GUC value, size of a single recovery queue in KB.
 */
int			recovery_queue_size_guc;

/*
 * Are TOAST trees consistent with primary indices.
 */
bool		toast_consistent = false;

/*
 * Checkpoint requests for flushing undo positions and their completion.
 */
RecoveryUndoLocFlush *recovery_undo_loc_flush;

/*
 * The last xmin we received from primary.
 */
OXid		recovery_xmin = InvalidOXid;

/*
 * Number of successfully finished recovery workers.
 */
pg_atomic_uint32 *worker_finish_count;
pg_atomic_uint32 *idx_worker_finish_count;
pg_atomic_uint32 *worker_ptrs_changes;
RecoveryWorkerPtrs *worker_ptrs;
pg_atomic_uint64 *recovery_ptr;
pg_atomic_uint64 *recovery_main_retain_ptr;
pg_atomic_uint64 *recovery_finished_list_ptr;
bool	   *recovery_single_process;
bool	   *was_in_recovery;
pg_atomic_uint32 *after_recovery_cleaned;

pg_atomic_uint64 *recovery_index_next_pos;
pg_atomic_uint64 *recovery_index_completed_pos;
ConditionVariable *recovery_index_cv;

static void delay_rels_queued_for_idxbuild(ORelOids oids);
static void delay_if_queued_for_idxbuild(void);
static void update_run_xmin(void);
static void free_run_xmin(void);
static bool need_flush_undo_pos(int worker_id);
static void flush_current_undo_stack(void);
static void o_handle_startup_proc_interrupts_hook(void);
static void abort_recovery(RecoveryWorkerState *workers_pool, bool send_to_idx_pool);

static bool replay_container(Pointer ptr, Pointer endPtr,
							 bool single, XLogRecPtr xlogRecPtr,
							 XLogRecPtr xlogRecEndPtr);

static void worker_send_modify(int worker_id, BTreeDescr *desc,
							   RecoveryMsgType recType,
							   OTuple tuple, int tuple_len);
static void workers_send_oxid_finish(XLogRecPtr ptr, bool commit);
static void workers_send_savepoint(SubTransactionId parentSubId);
static void workers_send_rollback_to_savepoint(XLogRecPtr ptr,
											   SubTransactionId parentSubId);
static void workers_synchronize(XLogRecPtr csn, bool send_synchronize);
static void workers_notify_toast_consistent(void);
static void worker_wait_shutdown(RecoveryWorkerState *worker);

static inline bool apply_sys_tree_modify_record(int sys_tree_num, uint16 type,
												OTuple tup,
												OXid oxid, CommitSeqNo csn);
static inline void spread_idx_modify(BTreeDescr *desc,
									 RecoveryMsgType recType,
									 OTuple rec);

static inline RecoveryMsgType recovery_msg_from_wal_record(uint8 wal_record);
static void recovery_send_init(int worker_num);

/*
 * Returns full size of the shared memory needed to recovery.
 */
Size
recovery_shmem_needs(void)
{
	Size		size = 0;

	size = add_size(size, mul_size(CACHELINEALIGN((Size) recovery_queue_size_guc * 1024),
								   recovery_pool_size_guc + recovery_idx_pool_size_guc));
	size = add_size(size, CACHELINEALIGN(sizeof(bool)));
	size = add_size(size, CACHELINEALIGN(sizeof(pg_atomic_uint32)));
	size = add_size(size, CACHELINEALIGN(sizeof(pg_atomic_uint32)));
	size = add_size(size, CACHELINEALIGN(sizeof(pg_atomic_uint32)));
	size = add_size(size, CACHELINEALIGN(sizeof(RecoveryUndoLocFlush)));
	size = add_size(size, CACHELINEALIGN(mul_size(sizeof(RecoveryWorkerPtrs),
												  recovery_pool_size_guc + recovery_idx_pool_size_guc)));
	size = add_size(size, CACHELINEALIGN(mul_size(sizeof(pg_atomic_uint64), 3)));
	size = add_size(size, CACHELINEALIGN(sizeof(bool)));
	size = add_size(size, CACHELINEALIGN(sizeof(pg_atomic_uint32)));
	size = add_size(size, CACHELINEALIGN(sizeof(pg_atomic_uint64)));
	size = add_size(size, CACHELINEALIGN(sizeof(pg_atomic_uint64)));
	size = add_size(size, CACHELINEALIGN(sizeof(ConditionVariable)));

	return size;
}

/*
 * Initializes recovery shared memory.
 *
 * Must be called after checkpoint_shmem_init() because it initializes
 * startupCommitSeqNo.
 */
void
recovery_shmem_init(Pointer ptr, bool found)
{
	recovery_queue_data_size = (Size) recovery_queue_size_guc * 1024;

	recovery_first_queue = ptr;
	ptr += mul_size(CACHELINEALIGN(recovery_queue_data_size),
					recovery_pool_size_guc + recovery_idx_pool_size_guc);

	recovery_single_process = (bool *) ptr;
	ptr += CACHELINEALIGN(sizeof(bool));

	worker_finish_count = (pg_atomic_uint32 *) ptr;
	ptr += CACHELINEALIGN(sizeof(pg_atomic_uint32));

	idx_worker_finish_count = (pg_atomic_uint32 *) ptr;
	ptr += CACHELINEALIGN(sizeof(pg_atomic_uint32));

	worker_ptrs_changes = (pg_atomic_uint32 *) ptr;
	ptr += CACHELINEALIGN(sizeof(pg_atomic_uint32));

	recovery_undo_loc_flush = (RecoveryUndoLocFlush *) ptr;
	ptr += CACHELINEALIGN(sizeof(RecoveryUndoLocFlush));

	worker_ptrs = (RecoveryWorkerPtrs *) ptr;
	ptr += CACHELINEALIGN(mul_size(sizeof(RecoveryWorkerPtrs), recovery_pool_size_guc + recovery_idx_pool_size_guc));

	recovery_ptr = (pg_atomic_uint64 *) ptr;
	recovery_main_retain_ptr = recovery_ptr + 1;
	recovery_finished_list_ptr = recovery_ptr + 2;

	ptr += CACHELINEALIGN(mul_size(sizeof(pg_atomic_uint64), 3));

	was_in_recovery = (bool *) ptr;
	ptr += CACHELINEALIGN(sizeof(bool));

	after_recovery_cleaned = (pg_atomic_uint32 *) ptr;
	ptr += CACHELINEALIGN(sizeof(pg_atomic_uint32));

	recovery_index_next_pos = (pg_atomic_uint64 *) ptr;
	ptr += CACHELINEALIGN(sizeof(pg_atomic_uint64));

	recovery_index_completed_pos = (pg_atomic_uint64 *) ptr;
	ptr += CACHELINEALIGN(sizeof(pg_atomic_uint64));

	recovery_index_cv = (ConditionVariable *) ptr;
	ptr += CACHELINEALIGN(sizeof(ConditionVariable));

	if (!found)
	{
		int			i;

		recovery_undo_loc_flush->finishRequestCheckpointNumber = 0;
		recovery_undo_loc_flush->immediateRequestCheckpointNumber = 0;
		recovery_undo_loc_flush->completedCheckpointNumber = UINT32_MAX;
		recovery_undo_loc_flush->recoveryMainCompletedCheckpointNumber = 0;
		SpinLockInit(&recovery_undo_loc_flush->exitLock);

		pg_atomic_init_u32(worker_finish_count, 0);
		pg_atomic_init_u32(idx_worker_finish_count, 0);
		pg_atomic_init_u32(worker_ptrs_changes, 0);

		for (i = 0; i < recovery_pool_size_guc + recovery_idx_pool_size_guc; i++)
		{
			shm_mq_create(GET_WORKER_QUEUE(i), recovery_queue_data_size);
			pg_atomic_init_u64(&worker_ptrs[i].commitPtr, InvalidXLogRecPtr);
			pg_atomic_init_u64(&worker_ptrs[i].retainPtr, InvalidXLogRecPtr);
			worker_ptrs[i].flushedUndoLocCompletedCheckpointNumber = 0;
		}
		pg_atomic_init_u64(recovery_ptr, InvalidXLogRecPtr);
		pg_atomic_init_u64(recovery_main_retain_ptr, InvalidXLogRecPtr);
		pg_atomic_init_u64(recovery_finished_list_ptr, InvalidXLogRecPtr);

		*was_in_recovery = false;
		pg_atomic_init_u32(after_recovery_cleaned, 0);

		pg_atomic_init_u64(recovery_index_next_pos, 0);
		pg_atomic_init_u64(recovery_index_completed_pos, 0);
		ConditionVariableInit(recovery_index_cv);
	}
}

static void
undo_stack_locations_set_invalid(UndoStackLocations *location)
{
	location->location = InvalidUndoLocation;
	location->subxactLocation = InvalidUndoLocation;
	location->branchLocation = InvalidUndoLocation;
	location->onCommitLocation = InvalidUndoLocation;
}

/*
 * Read information about undo locations of in-progress transactions.
 */
static void
read_xids(int checkpointnum, bool recovery_single, int worker_id)
{
	char	   *xidFilename = psprintf(XID_FILENAME_FORMAT, checkpointnum);
	File		xidFile;
	off_t		offset = 0;
	uint32		count = 0,
				i;

	xidFile = PathNameOpenFile(xidFilename, O_RDONLY | PG_BINARY);
	if (xidFile < 0)
		ereport(FATAL, (errcode_for_file_access(),
						errmsg("could not open xid file %s: %m", xidFilename)));

	if (OFileRead(xidFile, (Pointer) &count,
				  sizeof(count), offset,
				  WAIT_EVENT_SLRU_READ) != sizeof(count))
		ereport(FATAL, (errcode_for_file_access(),
						errmsg("could not read xid record from file %s: %m", xidFilename)));
	offset += sizeof(count);

	for (i = 0; i < count; i++)
	{
		RecoveryXidState *state;
		XidFileRec	xidRec = {0};
		bool		found;

		if (OFileRead(xidFile, (Pointer) &xidRec,
					  sizeof(xidRec), offset,
					  WAIT_EVENT_SLRU_READ) != sizeof(xidRec))
			ereport(FATAL, (errcode_for_file_access(),
							errmsg("could not read xid record from file %s: %m", xidFilename)));

		advance_oxids(xidRec.oxid);
		state = (RecoveryXidState *) hash_search(recovery_xid_state_hash,
												 &xidRec.oxid,
												 HASH_ENTER,
												 &found);

		if (!found)
		{
			int			j;

			state->xid = InvalidTransactionId;
			state->needs_wal_flush = false;
			for (j = 0; j < (int) UndoLogsCount; j++)
				state->retain_locs[j] = InvalidUndoLocation;	/* undo locations are
																 * held by checkpoint */
			state->csn = COMMITSEQNO_INPROGRESS;
			state->ptr = InvalidXLogRecPtr;
			state->in_finished_list = false;
			for (j = 0; j < (int) UndoLogsCount; j++)
				state->in_retain_undo_heaps[j] = false;
			for (j = 0; j < (int) UndoLogsCount; j++)
				undo_stack_locations_set_invalid(&state->undo_stacks[j]);
			dlist_init(&state->checkpoint_undo_stacks);
			if (worker_id < 0)
				pairingheap_add(xmin_queue, &state->xmin_ph_node);

			state->systree_modified = false;
			state->invalidate_typcache = false;
			state->o_tables_meta_locked = false;
			state->checkpoint_xid = true;
			state->wal_xid = false;
			if (!recovery_single && worker_id < 0)
				state->used_by = palloc0((recovery_pool_size_guc + recovery_idx_pool_size_guc) * sizeof(bool));
			else
				state->used_by = NULL;
		}
		if (worker_id < 0)
		{
			CheckpointUndoStack *stack;

			stack = (CheckpointUndoStack *) MemoryContextAlloc(TopMemoryContext,
															   sizeof(CheckpointUndoStack));
			stack->undoType = xidRec.undoType;
			stack->undoStack = xidRec.undoLocation;
			dlist_push_tail(&state->checkpoint_undo_stacks, &stack->node);
			set_oxid_csn(xidRec.oxid, COMMITSEQNO_INPROGRESS);
		}

		offset += sizeof(xidRec);
	}

	if (worker_id < 0)
		update_run_xmin();
	FileClose(xidFile);
	pfree(xidFilename);
}

/*
 * Apply undo records "hidden" in undo branches.
 *
 * These records are intended to be already aborted.  But checkpointer could
 * "see" tuples which still reference those records.  This routine is du
 */
static void
apply_xids_branches(void)
{
	HASH_SEQ_STATUS hash_seq;

	hash_seq_init(&hash_seq, recovery_xid_state_hash);
	while ((cur_state = (RecoveryXidState *) hash_seq_search(&hash_seq)) != NULL)
	{
		dlist_iter	iter;

		oxid_needs_wal_flush = cur_state->needs_wal_flush;
		recovery_oxid = cur_state->oxid;

		dlist_foreach(iter, &cur_state->checkpoint_undo_stacks)
		{
			CheckpointUndoStack *stack = dlist_container(CheckpointUndoStack,
														 node,
														 iter.cur);

			if ((int) (stack->undoType) < UndoLogsCount)
			{
				set_cur_undo_locations(stack->undoType, stack->undoStack);
				apply_undo_branches(stack->undoType, recovery_oxid);
			}
			else
			{
				uint64		location PG_USED_FOR_ASSERTS_ONLY;

				Assert(!UndoLocationIsValid(stack->undoStack.location));
				Assert(!UndoLocationIsValid(stack->undoStack.branchLocation));
				Assert(!UndoLocationIsValid(stack->undoStack.subxactLocation));
				location = walk_undo_range_with_buf((UndoLogType) ((int) (stack->undoType) - XID_REC_REWIND_TYPES_OFFSET),
													stack->undoStack.onCommitLocation,
													InvalidUndoLocation, recovery_oxid, false, NULL, true);
				/* NB rewindItem->oxid is not used in recovery */
				Assert(!UndoLocationIsValid(location));
			}
		}
	}

	oxid_needs_wal_flush = false;
	recovery_oxid = InvalidOXid;
	reset_cur_undo_locations();
	cur_state = NULL;
}

void
idx_workers_shutdown(void)
{
	int			i;

	workers_send_finish(true);
	for (i = index_build_first_worker; i <= index_build_last_worker; i++)
	{
		worker_wait_shutdown(&workers_pool[i]);
	}

	if (pg_atomic_read_u32(idx_worker_finish_count) != index_build_workers)
		elog(ERROR, "orioledb recovery idx worker died.");
}

void
o_recovery_start_hook(void)
{
	RecoveryWorkerState *state;
	int			i;
	bool		recovery_single;

	before_shmem_exit(recovery_on_proc_exit, (Datum) -1);
	recovery_single = *recovery_single_process = IsFatalError();
	if (recovery_single)
	{
		elog(LOG, "orioledb recovery after fatal error started.  Unable to make multiprocess recovery.");
	}
	else
	{
		elog(LOG, "orioledb recovery started.");
	}

	startup_chkp_num = checkpoint_state->lastCheckpointNumber;
	recovery_cleanup_old_files(startup_chkp_num, true);

	if (!recovery_single)
	{
		int			finish = recovery_idx_pool_size_guc ? index_build_leader : recovery_last_worker;

		workers_pool = palloc0(sizeof(RecoveryWorkerState) * (finish + 1));

		for (i = recovery_first_worker; i <= finish; i++)
		{
			state = &workers_pool[i];
			shm_mq_set_sender(GET_WORKER_QUEUE(i), MyProc);
			state->type = oIndexInvalid;
			state->oids.datoid = InvalidOid;
			state->oids.reloid = InvalidOid;
			state->oids.relnode = InvalidOid;
			state->oxid = InvalidOXid;

			workers_pool[i].handle = recovery_worker_register(i);
			if (workers_pool[i].handle == NULL)
			{
				/*
				 * Not enough slots for background workers.
				 */
				for (i--; i >= 0; i--)
					TerminateBackgroundWorker(workers_pool[i].handle);

				recovery_single = *recovery_single_process = true;
				finish = -1;

				ereport(WARNING,
						(errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
						 errmsg("unable to start recovery workers"),
						 errdetail("You must increase max_worker_processes value or decrease orioledb.recovery_pool_size value.  Fallback to recovery in single-process mode.")));

				break;
			}
			state->queue = shm_mq_attach(GET_WORKER_QUEUE(i), NULL, workers_pool[i].handle);
			state->queue_buf_len = 0;
		}
		for (i = recovery_first_worker; i <= finish; i++)
		{
			if (shm_mq_wait_for_attach(workers_pool[i].queue) != SHM_MQ_SUCCESS)
				elog(ERROR, "unable to attach recovery workers to shm queue");
			recovery_send_init(i);
		}
	}

/*	if (enable_stopevents)
	{
		wait_for_stopevent_enabled(STOPEVENT_RECOVERY_START);
		STOPEVENT(STOPEVENT_RECOVERY_START, NULL);
	}*/

	recovery_undo_loc_flush->completedCheckpointNumber = 0;

	pg_write_barrier();

	recovery_init(-1);

	if (checkpoint_state->lastCheckpointNumber > 0)
		apply_xids_branches();
}

void
orioledb_redo(XLogReaderState *record)
{
	Pointer		msg_start = (Pointer) XLogRecGetData(record);
	int			msg_len = XLogRecGetDataLen(record);
	bool		recovery_single;

	Assert((XLogRecGetInfo(record) & ~XLR_INFO_MASK) == ORIOLEDB_XLOG_CONTAINER);
	recovery_single = *recovery_single_process;

	if (record->ReadRecPtr >= checkpoint_state->controlToastConsistentPtr)
	{
		toast_consistent = true;
		if (!recovery_single)
			workers_notify_toast_consistent();
	}

	if (record->ReadRecPtr >= checkpoint_state->controlReplayStartPtr)
	{
		if (!replay_container(msg_start, msg_start + msg_len, recovery_single,
							  record->ReadRecPtr, record->EndRecPtr))
		{
			abort_recovery(workers_pool, false);
			elog(ERROR, "orioledb recovery worker failed to replay WAL container.");
		}
	}

	if (unexpected_worker_detach)
	{
		abort_recovery(workers_pool, false);
		elog(ERROR, "orioledb recovery worker detached unexpectedly.");
	}
}

void
o_recovery_finish_hook(bool cleanup)
{
	RecoveryWorkerState *state;
	int			i,
				num_workers = recovery_idx_pool_size_guc ? recovery_pool_size_guc + 1 : recovery_pool_size_guc;
	bool		recovery_single;

	recovery_single = *recovery_single_process;

	if (!recovery_single)
	{
		workers_send_finish(false);
		for (i = 0; i < num_workers; i++)
		{
			worker_wait_shutdown(&workers_pool[i]);
		}
	}

	recovery_finish(-1);

	if (!recovery_single)
	{
		for (i = 0; i < num_workers; i++)
		{
			state = &workers_pool[i];
			shm_mq_detach(state->queue);
		}
		pfree(workers_pool);
	}

	/* Release all the locks.  All of them are acquired at statement-level. */
	LockReleaseCurrentOwner(NULL, 0);

	/*
	 * No sense to check recovery_internal_error state, because shm_mq_sendv()
	 * can return SHM_MQ_DETACHED even if finish message was successfully
	 * sent.
	 */
	if (!recovery_single && pg_atomic_read_u32(worker_finish_count) != num_workers)
	{
		elog(ERROR, "orioledb recovery worker died.");
	}

	if (cleanup && remove_old_checkpoint_files)
		recovery_cleanup_old_files(startup_chkp_num, false);

	elog(LOG, "orioledb recovery finished.");
	recovery_undo_loc_flush->completedCheckpointNumber = UINT32_MAX;
}

static XLogRecPtr
get_workers_commit_ptr(void)
{
	static CommitSeqNo prev_ptr = InvalidXLogRecPtr;
	static uint64 prev_changes = UINT64_MAX;
	uint64		old_changes;

	/* fast check - nothing changed */
	old_changes = pg_atomic_read_u32(worker_ptrs_changes);
	if (old_changes == prev_changes)
		return prev_ptr;

	pg_read_barrier();

	/* we need to find a new ptr */
	while (true)
	{
		XLogRecPtr	min_ptr;
		uint64		new_changes;
		int			i;

		min_ptr = pg_atomic_read_u64(&worker_ptrs[0].commitPtr);
		for (i = 1; i < recovery_pool_size_guc; i++)
			min_ptr = Min(min_ptr, pg_atomic_read_u64(&worker_ptrs[i].commitPtr));

		pg_read_barrier();

		new_changes = pg_atomic_read_u32(worker_ptrs_changes);
		if (old_changes != new_changes)
		{
			old_changes = new_changes;
			pg_read_barrier();
			continue;
		}

		prev_changes = new_changes;
		prev_ptr = min_ptr;
		return prev_ptr;
	}
}

/*
 * Returns minimum ptr which is already reached by all recovery workers.
 */
XLogRecPtr
recovery_get_current_ptr(void)
{
	Assert(RecoveryInProgress());

	/* fast check - single process recovery */
	if (*recovery_single_process)
		return pg_atomic_read_u64(recovery_ptr);

	return get_workers_commit_ptr();
}

XLogRecPtr
recovery_get_effective_replay_ptr(void)
{
	XLogRecPtr	ptr,
				finishedPtr;

	if (!RecoveryInProgress() || *recovery_single_process)
		return InvalidXLogRecPtr;

	ptr = pg_atomic_read_u64(recovery_ptr);
	finishedPtr = pg_atomic_read_u64(recovery_finished_list_ptr);
	if (ptr == finishedPtr)
		return InvalidXLogRecPtr;
	else
		return finishedPtr;
}

static XLogRecPtr
recovery_get_retain_ptr(void)
{
	/* fast check - single process recovery */
	if (*recovery_single_process)
	{
		return pg_atomic_read_u64(recovery_ptr);
	}

	/* we need to find a new ptr */
	while (true)
	{
		XLogRecPtr	result;
		int			i;

		result = pg_atomic_read_u64(recovery_main_retain_ptr);
		for (i = 0; i < recovery_pool_size_guc; i++)
			result = Min(result, pg_atomic_read_u64(&worker_ptrs[i].retainPtr));

		return result;
	}
}

/*
 * Returns true if current process is recovery process.
 */
bool
is_recovery_process(void)
{
	return iam_recovery;
}

CommitSeqNo
recovery_map_oxid_csn(OXid oxid, bool *found)
{
	RecoveryXidState *state;

	state = hash_search(recovery_xid_state_hash, &oxid, HASH_FIND, found);
	if (*found)
	{
		if (!state->wal_xid)
			return COMMITSEQNO_ABORTED;
		return state->csn;
	}
	return 0;
}


/*
 * Initializes a new recovery process, recovery transaction support.
 */
void
recovery_init(int worker_id)
{
	HASHCTL		ctl;
	RecoveryWorkerState *state;
	int			i;

	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(OXid);
	ctl.entrysize = sizeof(RecoveryXidState);
	ctl.hcxt = TopMemoryContext;
	recovery_xid_state_hash = hash_create("orioledb recovery xid state hash",
										  16, &ctl,
										  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	iam_recovery = true;
	for (i = 0; i < (int) UndoLogsCount; i++)
	{
		retain_undo_queue_numbers[i] = i;
		retain_undo_queues[i] = pairingheap_allocate(retain_undo_pairingheap_cmp,
													 &retain_undo_queue_numbers[i]);
	}
	xmin_queue = pairingheap_allocate(xmin_pairingheap_cmp, NULL);
	dlist_init(&finished_list);
	dlist_init(&joint_commit_list);
	CurTransactionContext = AllocSetContextCreate(TopMemoryContext,
												  "orioledb recovery current transaction context",
												  ALLOCSET_DEFAULT_SIZES);
	TopTransactionContext = AllocSetContextCreate(TopMemoryContext,
												  "orioledb recovery top transaction context",
												  ALLOCSET_DEFAULT_SIZES);
	RelationCacheInitialize();	/* needed for OTableDescr invalidation */
	InitCatalogCache();

	o_set_syscache_hooks();

	if (checkpoint_state->lastCheckpointNumber > 0)
		read_xids(checkpoint_state->lastCheckpointNumber,
				  *recovery_single_process,
				  worker_id);

	if (worker_id < 0)
	{
		HASHCTL		reloid_ctl;

		MemSet(&reloid_ctl, 0, sizeof(reloid_ctl));
		reloid_ctl.keysize = sizeof(ORelOids);
		reloid_ctl.entrysize = sizeof(RecoveryIdxBuildQueueState);
		reloid_ctl.hcxt = TopMemoryContext;
		idxbuild_oids_hash = hash_create("orioledb recovery index build queue relations hash",
										 16, &reloid_ctl,
										 HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
		recovery_xmin = pg_atomic_read_u64(&xid_meta->runXmin);
	}

	if (worker_id == index_build_leader)
	{
		workers_pool = palloc0(sizeof(RecoveryWorkerState) * (recovery_idx_pool_size_guc + recovery_pool_size_guc));

		for (i = index_build_first_worker; i <= index_build_last_worker; i++)
		{
			state = &workers_pool[i];
			shm_mq_set_sender(GET_WORKER_QUEUE(i), MyProc);
			state->type = oIndexInvalid;
			state->oids.datoid = InvalidOid;
			state->oids.reloid = InvalidOid;
			state->oids.relnode = InvalidOid;
			state->oxid = InvalidOXid;

			workers_pool[i].handle = recovery_worker_register(i);

			if (workers_pool[i].handle == NULL)
			{
				/*
				 * Not enough slots for background workers.
				 */
				for (i--; i >= index_build_first_worker; i--)
					TerminateBackgroundWorker(workers_pool[i].handle);

				recovery_idx_pool_size_guc = 1;

				ereport(WARNING,
						(errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
						 errmsg("unable to start recovery workers"),
						 errdetail("You must increase max_worker_processes value or decrease orioledb.recovery_idx_pool_size value. Fallback to index build in single-process mode.")));
			}
			state->queue = shm_mq_attach(GET_WORKER_QUEUE(i), NULL, workers_pool[i].handle);
			state->queue_buf_len = 0;
		}

		for (i = index_build_first_worker; i <= index_build_last_worker; i++)
		{
			if (shm_mq_wait_for_attach(workers_pool[i].queue) != SHM_MQ_SUCCESS)
				elog(ERROR, "unable to attach recovery workers to shm queue");
			recovery_send_init(i);
		}
	}

	HandleStartupProcInterrupts_hook = o_handle_startup_proc_interrupts_hook;
}

static void
walk_checkpoint_stacks(CommitSeqNo csn,
					   SubTransactionId parentSubid,
					   bool flushUndoPos)
{
	dlist_mutable_iter miter;

	oxid_needs_wal_flush = cur_state->needs_wal_flush;
	recovery_oxid = cur_state->oxid;

	dlist_foreach_modify(miter, &cur_state->checkpoint_undo_stacks)
	{
		CheckpointUndoStack *stack = dlist_container(CheckpointUndoStack,
													 node,
													 miter.cur);

		if ((int) (stack->undoType) < UndoLogsCount)
		{
			set_cur_undo_locations(stack->undoType, stack->undoStack);
			if (flushUndoPos)
				flush_current_undo_stack();
			if (COMMITSEQNO_IS_ABORTED(csn))
			{
				if (parentSubid == InvalidSubTransactionId)
					apply_undo_stack(stack->undoType, recovery_oxid,
									 NULL, false);
				else
					rollback_to_savepoint(stack->undoType, UndoStackHead,
										  parentSubid, false);
			}
			else
			{
				on_commit_undo_stack(stack->undoType, recovery_oxid, false);
			}
		}
		dlist_delete(miter.cur);
		pfree(stack);
	}
}

/*
 * Finishes a recovery process, close all recovery transactions.
 */
void
recovery_finish(int worker_id)
{
	bool		flush_undo_pos = need_flush_undo_pos(worker_id);
	HASH_SEQ_STATUS hash_seq;
	int			i;

	delay_if_queued_for_idxbuild();

	if (cur_state)
	{
		cur_state->needs_wal_flush = oxid_needs_wal_flush;
		for (i = 0; i < (int) UndoLogsCount; i++)
		{
			cur_state->undo_stacks[i] = get_cur_undo_locations((UndoLogType) i);
			cur_state->retain_locs[i] = curRetainUndoLocations[i];
		}
	}

	hash_seq_init(&hash_seq, recovery_xid_state_hash);
	while ((cur_state = (RecoveryXidState *) hash_seq_search(&hash_seq)) != NULL)
	{
		if (cur_state->o_tables_meta_locked)
		{
			o_tables_meta_unlock_no_wal();
			cur_state->o_tables_meta_locked = false;
		}

		if (COMMITSEQNO_IS_INPROGRESS(cur_state->csn))
		{
			oxid_needs_wal_flush = cur_state->needs_wal_flush;
			recovery_oxid = cur_state->oxid;
			for (i = 0; i < (int) UndoLogsCount; i++)
				set_cur_undo_locations((UndoLogType) i, cur_state->undo_stacks[i]);
			if (flush_undo_pos)
				flush_current_undo_stack();
			for (i = 0; i < (int) UndoLogsCount; i++)
				apply_undo_stack((UndoLogType) i, recovery_oxid, NULL, true);
			walk_checkpoint_stacks(COMMITSEQNO_ABORTED,
								   InvalidSubTransactionId,
								   flush_undo_pos);
		}
		if (cur_state->in_finished_list && worker_id < 0)
		{
			set_oxid_csn(cur_state->oxid, COMMITSEQNO_COMMITTING);
			cur_state->csn = pg_atomic_fetch_add_u64(&TRANSAM_VARIABLES->nextCommitSeqNo, 1);
			set_oxid_csn(cur_state->oxid, cur_state->csn);
		}
		if (cur_state->used_by)
			pfree(cur_state->used_by);
	}
	HandleStartupProcInterrupts_hook = NULL;
	hash_destroy(recovery_xid_state_hash);
	recovery_xid_state_hash = NULL;

	if (worker_id < 0)
	{
		hash_destroy(idxbuild_oids_hash);
		idxbuild_oids_hash = NULL;
	}
	for (i = 0; i < (int) UndoLogsCount; i++)
	{
		release_undo_size((UndoLogType) i);
		free_retained_undo_location((UndoLogType) i);
		pairingheap_free(retain_undo_queues[i]);
	}
	if (worker_id < 0)
	{
		pairingheap_free(xmin_queue);
		free_run_xmin();
	}
	if (worker_id >= 0)
		pg_atomic_write_u64(&worker_ptrs[worker_id].retainPtr,
							pg_atomic_read_u64(&worker_ptrs[worker_id].commitPtr));
	else
		pg_atomic_write_u64(recovery_main_retain_ptr,
							pg_atomic_read_u64(recovery_ptr));

	oxid_needs_wal_flush = false;
	recovery_oxid = InvalidOXid;
	reset_cur_undo_locations();
	MemoryContextDelete(CurTransactionContext);
	MemoryContextDelete(TopTransactionContext);
	TopTransactionContext = NULL;
	CurTransactionContext = NULL;
	iam_recovery = false;

	o_unset_syscache_hooks();
}

/*
 * Switches recovery process to other orioledb transaction.
 */
void
recovery_switch_to_oxid(OXid oxid, int worker_id)
{
	int			i;

	if (recovery_oxid != oxid)
	{
		bool		found;

		if (cur_state)
		{
			cur_state->needs_wal_flush = oxid_needs_wal_flush;
			for (i = 0; i < (int) UndoLogsCount; i++)
			{
				cur_state->undo_stacks[i] = get_cur_undo_locations((UndoLogType) i);

				if (!UndoLocationIsValid(cur_state->retain_locs[i]) &&
					UndoLocationIsValid(curRetainUndoLocations[i]))
				{
					cur_state->retain_locs[i] = curRetainUndoLocations[i];
					cur_state->in_retain_undo_heaps[i] = true;
					pairingheap_add(retain_undo_queues[i], &cur_state->retain_undo_ph_nodes[i]);
				}
			}
		}

		recovery_oxid = oxid;
		cur_state = (RecoveryXidState *) hash_search(recovery_xid_state_hash,
													 &oxid,
													 HASH_ENTER,
													 &found);
		cur_state->wal_xid = true;

		if (found)
		{
			oxid_needs_wal_flush = cur_state->needs_wal_flush;
			for (i = 0; i < (int) UndoLogsCount; i++)
			{
				set_cur_undo_locations((UndoLogType) i, cur_state->undo_stacks[i]);
				curRetainUndoLocations[i] = cur_state->retain_locs[i];
			}
		}
		else
		{
			cur_state->xid = InvalidTransactionId;
			for (i = 0; i < (int) UndoLogsCount; i++)
			{
				cur_state->retain_locs[i] = InvalidUndoLocation;
				cur_state->in_retain_undo_heaps[i] = false;
			}
			cur_state->csn = COMMITSEQNO_INPROGRESS;
			cur_state->ptr = InvalidXLogRecPtr;
			cur_state->needs_wal_flush = false;
			cur_state->in_finished_list = false;
			dlist_init(&cur_state->checkpoint_undo_stacks);
			oxid_needs_wal_flush = false;
			reset_cur_undo_locations();
			for (i = 0; i < (int) UndoLogsCount; i++)
				curRetainUndoLocations[i] = InvalidUndoLocation;
			if (worker_id < 0)
				pairingheap_add(xmin_queue, &cur_state->xmin_ph_node);
			cur_state->systree_modified = false;
			cur_state->invalidate_typcache = false;
			cur_state->o_tables_meta_locked = false;
			cur_state->checkpoint_xid = false;
			if (worker_id < 0 && !*recovery_single_process)
				cur_state->used_by = palloc0((recovery_pool_size_guc + recovery_idx_pool_size_guc) *
											 sizeof(bool));
			else
				cur_state->used_by = NULL;
		}
		update_proc_retain_undo_location(worker_id);
	}
}

/*
 * Delete recovery xid item if it's already deleted from both retain undo
 * location heap and finished list.
 */
static void
check_delete_xid_state(RecoveryXidState *state, int worker_id)
{
	int			i;
	bool		in_retain_heaps = false;

	for (i = 0; i < (int) UndoLogsCount; i++)
		if (state->in_retain_undo_heaps[i])
			in_retain_heaps = true;

	if (!in_retain_heaps &&
		!state->in_finished_list)
	{
		OXid		oxid = state->oxid;
		bool		found;

		if (state->used_by)
			pfree(state->used_by);
		if (worker_id < 0)
		{
			pairingheap_remove(xmin_queue, &state->xmin_ph_node);
			update_run_xmin();
		}
		hash_search(recovery_xid_state_hash, &oxid, HASH_REMOVE, &found);
		Assert(found);
	}
}

static bool
need_flush_undo_pos(int worker_id)
{
	if (worker_id < 0)
	{
		return recovery_undo_loc_flush->recoveryMainCompletedCheckpointNumber <
			recovery_undo_loc_flush->finishRequestCheckpointNumber;
	}
	else
	{
		return worker_ptrs[worker_id].flushedUndoLocCompletedCheckpointNumber <
			recovery_undo_loc_flush->finishRequestCheckpointNumber;
	}
}

static void
flush_current_undo_stack(void)
{
	XidFileRec	rec;
	int			i;

	rec.oxid = recovery_oxid;
	for (i = 0; i < (int) UndoLogsCount; i++)
	{
		rec.undoType = (UndoLogType) i;
		rec.undoLocation = get_cur_undo_locations((UndoLogType) i);
		write_to_xids_queue(&rec);
	}
}

/*
 * Finishes the current recovery transaction for the current recovery process.
 */
void
recovery_finish_current_oxid(CommitSeqNo csn, XLogRecPtr ptr,
							 int worker_id, bool sync)
{
	OXid		oxid = recovery_oxid;
	bool		flush_undo_pos = need_flush_undo_pos(worker_id);
	int			i;

	delay_if_queued_for_idxbuild();

	if (!COMMITSEQNO_IS_ABORTED(csn) && sync)
	{
		Assert(worker_id < 0);
		set_oxid_csn(oxid, COMMITSEQNO_COMMITTING);
		if (flush_undo_pos)
			flush_current_undo_stack();
		for (i = 0; i < (int) UndoLogsCount; i++)
			on_commit_undo_stack((UndoLogType) i, oxid, true);
		walk_checkpoint_stacks(csn, InvalidSubTransactionId, flush_undo_pos);
		csn = pg_atomic_fetch_add_u64(&TRANSAM_VARIABLES->nextCommitSeqNo, 1);
		set_oxid_csn(oxid, csn);
		set_oxid_xlog_ptr(oxid, XLOG_PTR_ALIGN(ptr));
	}
	else if (!COMMITSEQNO_IS_ABORTED(csn) && !sync)
	{
		if (flush_undo_pos)
			flush_current_undo_stack();
		for (i = 0; i < (int) UndoLogsCount; i++)
			on_commit_undo_stack((UndoLogType) i, oxid, true);
		walk_checkpoint_stacks(csn, InvalidSubTransactionId, flush_undo_pos);
		cur_state->in_finished_list = true;
		dlist_push_tail(&finished_list, &cur_state->finished_list_node);
	}
	else
	{
		if (flush_undo_pos)
			flush_current_undo_stack();
		for (i = 0; i < (int) UndoLogsCount; i++)
			apply_undo_stack((UndoLogType) i, oxid, NULL, true);
		walk_checkpoint_stacks(csn, InvalidSubTransactionId, flush_undo_pos);
		if (worker_id < 0)
		{
			if (sync)
			{
				set_oxid_csn(oxid, COMMITSEQNO_ABORTED);
				set_oxid_xlog_ptr(oxid, InvalidXLogRecPtr);
			}
			else
			{
				/*
				 * Postpone transaction abort until it will be aborted by all
				 * the workers.  Otherwise, workers can consider it as
				 * committed due to runXmin.
				 */
				cur_state->in_finished_list = true;
				dlist_push_tail(&finished_list, &cur_state->finished_list_node);
			}
		}
	}

	cur_state->csn = csn;
	cur_state->ptr = ptr;

	if (cur_state->o_tables_meta_locked)
	{
		o_tables_meta_unlock_no_wal();
		cur_state->o_tables_meta_locked = false;
	}

	oxid_needs_wal_flush = false;
	reset_cur_undo_locations();
	recovery_oxid = InvalidOXid;

	for (i = 0; i < (int) UndoLogsCount; i++)
	{
		if (!UndoLocationIsValid(cur_state->retain_locs[i]) &&
			UndoLocationIsValid(curRetainUndoLocations[i]))
		{
			cur_state->retain_locs[i] = curRetainUndoLocations[i];
			pairingheap_add(retain_undo_queues[i], &cur_state->retain_undo_ph_nodes[i]);
			cur_state->in_retain_undo_heaps[i] = true;
		}
		curRetainUndoLocations[i] = InvalidUndoLocation;
	}

	for (i = 0; i < (int) UndoLogsCount; i++)
		release_undo_size((UndoLogType) i);
	check_delete_xid_state(cur_state, worker_id);

	cur_state = NULL;

	update_proc_retain_undo_location(worker_id);
}

static void
checkpoint_rollback_to_savepoint(SubTransactionId parentSubid)
{
	int			i;

	for (i = 0; i < (int) UndoLogsCount; i++)
		cur_state->undo_stacks[i] = get_cur_undo_locations((UndoLogType) i);
	walk_checkpoint_stacks(COMMITSEQNO_ABORTED, parentSubid, false);
	for (i = 0; i < (int) UndoLogsCount; i++)
		set_cur_undo_locations((UndoLogType) i, cur_state->undo_stacks[i]);
}

void
recovery_savepoint(SubTransactionId parentSubid, int worker_id)
{
	if (worker_id == -1)
		checkpoint_rollback_to_savepoint(parentSubid);

	add_subxact_undo_item(parentSubid);
}

void
recovery_rollback_to_savepoint(SubTransactionId parentSubid, int worker_id)
{
	int			i;

	for (i = 0; i < (int) UndoLogsCount; i++)
		rollback_to_savepoint((UndoLogType) i, UndoStackTail,
							  parentSubid, true);

	if (worker_id == -1)
		checkpoint_rollback_to_savepoint(parentSubid);
}

OBTreeModifyCallbackAction
recovery_insert_primary_callback(BTreeDescr *descr,
								 OTuple tup, OTuple *newtup, OXid oxid,
								 OTupleXactInfo xactInfo,
								 UndoLocation location, RowLockMode *lock_mode,
								 BTreeLocationHint *hint, void *arg)
{
	if (XACT_INFO_OXID_EQ(xactInfo, oxid) &&
		o_tuple_get_version(tup) >= o_tuple_get_version(*newtup))
		return OBTreeCallbackActionUndo;
	return OBTreeCallbackActionUpdate;
}

OBTreeModifyCallbackAction
recovery_delete_primary_callback(BTreeDescr *descr,
								 OTuple tup, OTuple *newtup, OXid oxid,
								 OTupleXactInfo xactInfo,
								 UndoLocation location,
								 RowLockMode *lock_mode,
								 BTreeLocationHint *hint, void *arg)
{
	OTuple	   *key = (OTuple *) arg;

	if (XACT_INFO_OXID_EQ(xactInfo, oxid) &&
		o_tuple_get_version(tup) > o_tuple_get_version(*key))
		return OBTreeCallbackActionUndo;

	return OBTreeCallbackActionDelete;
}

OBTreeModifyCallbackAction
recovery_insert_overwrite_callback(BTreeDescr *descr,
								   OTuple tup, OTuple *newtup, OXid oxid,
								   OTupleXactInfo xactInfo,
								   UndoLocation location,
								   RowLockMode *lock_mode,
								   BTreeLocationHint *hint, void *arg)
{
	if (XACT_INFO_OXID_EQ(xactInfo, oxid))
		return OBTreeCallbackActionUndo;

	return OBTreeCallbackActionUpdate;
}

OBTreeModifyCallbackAction
recovery_delete_overwrite_callback(BTreeDescr *descr,
								   OTuple tup, OTuple *newtup, OXid oxid,
								   OTupleXactInfo xactInfo,
								   UndoLocation location,
								   RowLockMode *lock_mode,
								   BTreeLocationHint *hint, void *arg)
{
	if (XACT_INFO_OXID_EQ(xactInfo, oxid))
		return OBTreeCallbackActionUndo;

	return OBTreeCallbackActionDelete;
}

static OBTreeModifyCallbackAction
recovery_insert_systree_callback(BTreeDescr *descr,
								 OTuple tup, OTuple *newtup, OXid oxid,
								 OTupleXactInfo xactInfo,
								 UndoLocation location, RowLockMode *lock_mode,
								 BTreeLocationHint *hint, void *arg)
{
	return OBTreeCallbackActionUpdate;
}

OBTreeModifyCallbackAction
recovery_insert_deleted_primary_callback(BTreeDescr *descr,
										 OTuple tup, OTuple *newtup, OXid oxid,
										 OTupleXactInfo xactInfo,
										 BTreeLeafTupleDeletedStatus deleted,
										 UndoLocation location, RowLockMode *lock_mode,
										 BTreeLocationHint *hint, void *arg)
{
	if (XACT_INFO_OXID_EQ(xactInfo, oxid) &&
		o_tuple_get_version(tup) >= o_tuple_get_version(*newtup))
		return OBTreeCallbackActionUndo;
	return OBTreeCallbackActionUpdate;
}

OBTreeModifyCallbackAction
recovery_delete_deleted_primary_callback(BTreeDescr *descr,
										 OTuple tup, OTuple *newtup, OXid oxid,
										 OTupleXactInfo xactInfo,
										 BTreeLeafTupleDeletedStatus deleted,
										 UndoLocation location,
										 RowLockMode *lock_mode,
										 BTreeLocationHint *hint, void *arg)
{
	OTuple	   *key = (OTuple *) arg;

	if (XACT_INFO_OXID_EQ(xactInfo, oxid) &&
		o_tuple_get_version(tup) > o_tuple_get_version(*key))
		return OBTreeCallbackActionUndo;

	return OBTreeCallbackActionDelete;
}

OBTreeModifyCallbackAction
recovery_insert_deleted_overwrite_callback(BTreeDescr *descr,
										   OTuple tup, OTuple *newtup, OXid oxid,
										   OTupleXactInfo xactInfo,
										   BTreeLeafTupleDeletedStatus deleted,
										   UndoLocation location,
										   RowLockMode *lock_mode,
										   BTreeLocationHint *hint, void *arg)
{
	if (XACT_INFO_OXID_EQ(xactInfo, oxid))
		return OBTreeCallbackActionUndo;

	return OBTreeCallbackActionUpdate;
}

OBTreeModifyCallbackAction
recovery_delete_deleted_overwrite_callback(BTreeDescr *descr,
										   OTuple tup, OTuple *newtup, OXid oxid,
										   OTupleXactInfo xactInfo,
										   BTreeLeafTupleDeletedStatus deleted,
										   UndoLocation location,
										   RowLockMode *lock_mode,
										   BTreeLocationHint *hint, void *arg)
{
	if (XACT_INFO_OXID_EQ(xactInfo, oxid))
		return OBTreeCallbackActionUndo;

	return OBTreeCallbackActionDelete;
}

static OBTreeModifyCallbackAction
recovery_insert_deleted_systree_callback(BTreeDescr *descr,
										 OTuple tup, OTuple *newtup, OXid oxid,
										 OTupleXactInfo xactInfo,
										 BTreeLeafTupleDeletedStatus deleted,
										 UndoLocation location, RowLockMode *lock_mode,
										 BTreeLocationHint *hint, void *arg)
{
	return OBTreeCallbackActionUpdate;
}

/*
 * Applies modify recovery record to the BTree.
 */
bool
apply_btree_modify_record(BTreeDescr *tree, RecoveryMsgType type,
						  OTuple ptr, OXid oxid, CommitSeqNo csn)
{
	OBTreeModifyResult modifyResult;
	BTreeModifyCallbackInfo callbackInfo = nullCallbackInfo;
	bool		result;

	callbackInfo.arg = &ptr;

	if (IS_SYS_TREE_OIDS(tree->oids))
	{
		if (type == RecoveryMsgTypeInsert || type == RecoveryMsgTypeUpdate)
		{
			callbackInfo.modifyCallback = recovery_insert_systree_callback;
			callbackInfo.modifyDeletedCallback = recovery_insert_deleted_systree_callback;
		}
	}
	else if (tree->type == oIndexPrimary || tree->type == oIndexToast || tree->type == oIndexBridge)
	{
		if (type == RecoveryMsgTypeInsert || type == RecoveryMsgTypeUpdate)
		{
			callbackInfo.modifyCallback = recovery_insert_primary_callback;
			callbackInfo.modifyDeletedCallback = recovery_insert_deleted_primary_callback;
		}
		else if (type == RecoveryMsgTypeDelete)
		{
			callbackInfo.modifyCallback = recovery_delete_primary_callback;
			callbackInfo.modifyDeletedCallback = recovery_delete_deleted_primary_callback;
		}
	}
	else
	{
		if (type == RecoveryMsgTypeInsert || type == RecoveryMsgTypeUpdate)
		{
			callbackInfo.modifyCallback = recovery_insert_overwrite_callback;
			callbackInfo.modifyDeletedCallback = recovery_insert_deleted_overwrite_callback;
		}
		else if (type == RecoveryMsgTypeDelete)
		{
			callbackInfo.modifyCallback = recovery_delete_overwrite_callback;
			callbackInfo.modifyDeletedCallback = recovery_delete_deleted_overwrite_callback;
		}
	}

	switch (type)
	{
		case RecoveryMsgTypeInsert:
			modifyResult = o_btree_modify(tree, BTreeOperationInsert,
										  ptr, BTreeKeyLeafTuple,
										  NULL, BTreeKeyNone,
										  oxid, csn, RowLockUpdate,
										  NULL, &callbackInfo);
			result = modifyResult == OBTreeModifyResultInserted || modifyResult == OBTreeModifyResultUpdated;
			break;
		case RecoveryMsgTypeUpdate:
			result = o_btree_modify(tree, BTreeOperationInsert,
									ptr, BTreeKeyLeafTuple,
									NULL, BTreeKeyNone,
									oxid, csn, RowLockNoKeyUpdate,
									NULL, &callbackInfo) == OBTreeModifyResultUpdated;
			break;
		case RecoveryMsgTypeDelete:
			result = o_btree_modify(tree, BTreeOperationDelete,
									ptr, BTreeKeyNonLeafKey,
									NULL, BTreeKeyNone, oxid, csn, RowLockUpdate,
									NULL, &callbackInfo) == OBTreeModifyResultDeleted;
			break;
		default:
			Assert(false);
			elog(ERROR, "Wrong recovery record type %d", type);
	}

	return result;
}

void
replay_erase_bridge_item(OIndexDescr *bridge, ItemPointer iptr)
{
	OBTreeFindPageContext context;
	OBTreeKeyBound bound;
	OBtreePageFindItem *item;
	OFindPageResult findResult PG_USED_FOR_ASSERTS_ONLY;
	OTuple		tuple;
	Page		p;

	bound.nkeys = 1;
	bound.n_row_keys = 0;
	bound.row_keys = NULL;
	bound.keys[0].type = TIDOID;
	bound.keys[0].flags = O_VALUE_BOUND_PLAIN_VALUE;
	bound.keys[0].comparator = bridge->fields[0].comparator;
	bound.keys[0].value = ItemPointerGetDatum(iptr);

	init_page_find_context(&context, &bridge->desc,
						   COMMITSEQNO_INPROGRESS,
						   BTREE_PAGE_FIND_MODIFY);

	findResult = find_page(&context, &bound, BTreeKeyBound, 0);
	Assert(findResult == OFindPageResultSuccess);

	item = &context.items[context.index];
	p = O_GET_IN_MEMORY_PAGE(item->blkno);

	if (!BTREE_PAGE_LOCATOR_IS_VALID(p, &item->locator))
	{
		unlock_page(context.items[context.index].blkno);
		return;
	}

	BTREE_PAGE_READ_TUPLE(tuple, p, &item->locator);

	if (o_btree_cmp(&bridge->desc,
					&bound, BTreeKeyBound,
					&tuple, BTreeKeyLeafTuple) != 0)
	{
		unlock_page(context.items[context.index].blkno);
		return;
	}

	START_CRIT_SECTION();
	page_block_reads(item->blkno);
	page_locator_delete_item(p, &item->locator);
	MARK_DIRTY(&bridge->desc, item->blkno);
	END_CRIT_SECTION();
	unlock_page(context.items[context.index].blkno);
}

OTuple
recovery_rec_insert(BTreeDescr *desc, OTuple tuple, bool *allocated, int *size)
{
	*allocated = false;
	*size = o_btree_len(desc, tuple, OTupleLength);
	return tuple;
}

OTuple
recovery_rec_update(BTreeDescr *desc, OTuple tuple, bool *allocated, int *size)
{
	*allocated = false;
	*size = o_btree_len(desc, tuple, OTupleLength);
	return tuple;
}

OTuple
recovery_rec_delete(BTreeDescr *desc, OTuple tuple, bool *allocated, int *size)
{
	OTuple		key;

	key = o_btree_tuple_make_key(desc, tuple, NULL, true, allocated);
	*size = o_btree_len(desc, key, OKeyLength);
	return key;
}

OTuple
recovery_rec_delete_key(BTreeDescr *desc, OTuple key, bool *allocated, int *size)
{
	*allocated = false;
	*size = o_btree_len(desc, key, OKeyLength);
	return key;
}

/*
 * Debug method checks is recovery main process and recovery workers
 * transactions is synchronized.
 */
Datum
orioledb_recovery_synchronized(PG_FUNCTION_ARGS)
{
	XLogRecPtr	ptr = pg_atomic_read_u64(recovery_ptr);

	if (!ptr)
		PG_RETURN_BOOL(true);

	if (ptr != recovery_get_current_ptr())
		PG_RETURN_BOOL(false);

	if (ptr != recovery_get_retain_ptr())
		PG_RETURN_BOOL(false);

	WakeupRecovery();

	if (ptr != pg_atomic_read_u64(recovery_finished_list_ptr))
		PG_RETURN_BOOL(false);

	PG_RETURN_BOOL(true);
}

static void
update_run_xmin(void)
{
	OXid		xmin;

	if (!pairingheap_is_empty(xmin_queue))
	{
		RecoveryXidState *state;

		state = pairingheap_container(RecoveryXidState, xmin_ph_node,
									  pairingheap_first(xmin_queue));
		xmin = state->oxid;
	}
	else
	{
		xmin = pg_atomic_read_u64(&xid_meta->nextXid);
	}
	xmin = Min(xmin, recovery_xmin);
	pg_atomic_write_u64(&xid_meta->runXmin, xmin);
	if (xmin < pg_atomic_read_u64(&xid_meta->globalXmin))
		pg_atomic_write_u64(&xid_meta->globalXmin, xmin);
}

static void
free_run_xmin(void)
{
	OXid		xmin;

	xmin = pg_atomic_read_u64(&xid_meta->nextXid);
	pg_atomic_write_u64(&xid_meta->runXmin, xmin);
	if (xmin < pg_atomic_read_u64(&xid_meta->globalXmin))
		pg_atomic_write_u64(&xid_meta->globalXmin, xmin);
}

static bool
update_retain_location_with_heap(UndoLogType undoType, int worker_id,
								 XLogRecPtr recoveryPtr)
{
	ODBProcData *curProcData = GET_CUR_PROCDATA();
	RecoveryXidState *state;

	if (pairingheap_is_empty(retain_undo_queues[undoType]))
		return false;

	state = RetainUndoNodeGetRecoveryXidState(pairingheap_first(retain_undo_queues[undoType]), undoType);

	if (state->retain_locs[undoType] > pg_atomic_read_u64(&curProcData->undoRetainLocations[undoType].transactionUndoRetainLocation))
		pg_atomic_write_u64(&curProcData->undoRetainLocations[undoType].transactionUndoRetainLocation, state->retain_locs[undoType]);
	if (state->csn == COMMITSEQNO_ABORTED ||
		(COMMITSEQNO_IS_NORMAL(state->csn) && !state->in_finished_list && state->ptr <= recoveryPtr))
	{
		pairingheap_remove(retain_undo_queues[undoType], &state->retain_undo_ph_nodes[undoType]);
		state->in_retain_undo_heaps[undoType] = false;
		check_delete_xid_state(state, worker_id);
		return true;
	}
	else
	{
		return false;
	}
}

/*
 * Updates advanceReservedLocation for a recovery process. Searches min
 * transactionUndoRetainLocation for active transactions.
 */
void
update_proc_retain_undo_location(int worker_id)
{
	XLogRecPtr	recoveryPtr = InvalidXLogRecPtr,
				listPtr;
	RecoveryXidState *state;
	dlist_mutable_iter miter;
	int			i;
	bool		allRetainQueuesEmpty = true;

	for (i = 0; i < (int) UndoLogsCount; i++)
	{
		if (cur_state &&
			!UndoLocationIsValid(cur_state->retain_locs[i]) &&
			UndoLocationIsValid(curRetainUndoLocations[i]))
		{
			cur_state->retain_locs[i] = curRetainUndoLocations[i];
			cur_state->in_retain_undo_heaps[i] = true;
			pairingheap_add(retain_undo_queues[i], &cur_state->retain_undo_ph_nodes[i]);
		}
	}

	if (worker_id < 0)
		listPtr = recoveryPtr = recovery_get_current_ptr();
	else
		listPtr = pg_atomic_read_u64(recovery_finished_list_ptr);

	dlist_foreach_modify(miter, &finished_list)
	{
		state = dlist_container(RecoveryXidState, finished_list_node, miter.cur);
		if (state->ptr > listPtr)
			break;

		if (worker_id < 0)
		{
			if (!COMMITSEQNO_IS_ABORTED(state->csn))
			{
				set_oxid_csn(state->oxid, COMMITSEQNO_COMMITTING);
				state->csn = pg_atomic_fetch_add_u64(&TRANSAM_VARIABLES->nextCommitSeqNo, 1);
				set_oxid_csn(state->oxid, state->csn);
				set_oxid_xlog_ptr(state->oxid, XLOG_PTR_ALIGN(state->ptr));
			}
			else
			{
				set_oxid_csn(state->oxid, COMMITSEQNO_ABORTED);
				set_oxid_xlog_ptr(state->oxid, InvalidXLogRecPtr);
			}
		}
		dlist_delete(miter.cur);
		state->in_finished_list = false;
		check_delete_xid_state(state, worker_id);
	}
	if (worker_id < 0)
		pg_atomic_write_u64(recovery_finished_list_ptr, recoveryPtr);

	/*
	 * Remove transactions, visible for all, from the retain queue.
	 */
	for (i = 0; i < (int) UndoLogsCount; i++)
	{
		if (pairingheap_is_empty(retain_undo_queues[i]))
			free_retained_undo_location(i);
		else
			allRetainQueuesEmpty = false;
	}

	if (allRetainQueuesEmpty)
	{
		if (worker_id >= 0)
			pg_atomic_write_u64(&worker_ptrs[worker_id].retainPtr,
								pg_atomic_read_u64(&worker_ptrs[worker_id].commitPtr));
		else
			pg_atomic_write_u64(recovery_main_retain_ptr,
								pg_atomic_read_u64(recovery_ptr));
		return;
	}

	if (XLogRecPtrIsInvalid(recoveryPtr))
		recoveryPtr = recovery_get_current_ptr();

	while (true)
	{
		bool		removed = false;

		allRetainQueuesEmpty = true;
		for (i = 0; i < (int) UndoLogsCount; i++)
		{
			if (pairingheap_is_empty(retain_undo_queues[i]))
				free_retained_undo_location(i);
			else
				allRetainQueuesEmpty = false;
		}

		if (allRetainQueuesEmpty)
		{
			if (worker_id >= 0)
				pg_atomic_write_u64(&worker_ptrs[worker_id].retainPtr,
									pg_atomic_read_u64(&worker_ptrs[worker_id].commitPtr));
			else
				pg_atomic_write_u64(recovery_main_retain_ptr,
									pg_atomic_read_u64(recovery_ptr));
			return;
		}

		for (i = 0; i < (int) UndoLogsCount; i++)
		{
			bool		result;

			result = update_retain_location_with_heap(i, worker_id, recoveryPtr);
			removed = removed || result;
		}

		if (!removed)
			break;

	}
	if (worker_id >= 0)
		pg_atomic_write_u64(&worker_ptrs[worker_id].retainPtr, recoveryPtr);
	else
		pg_atomic_write_u64(recovery_main_retain_ptr, recoveryPtr);
}

static void
recovery_write_to_xids_queue(int worker_id, uint32 requestNumber)
{
	HASH_SEQ_STATUS hash_seq;
	RecoveryXidState *state;
	int			i;

	if (cur_state)
	{
		for (i = 0; i < (int) UndoLogsCount; i++)
			cur_state->undo_stacks[i] = get_cur_undo_locations((UndoLogType) i);
	}

	hash_seq_init(&hash_seq, recovery_xid_state_hash);
	while ((state = (RecoveryXidState *) hash_seq_search(&hash_seq)) != NULL)
	{
		XidFileRec	rec;
		dlist_iter	iter;

		if (!COMMITSEQNO_IS_INPROGRESS(state->csn))
			continue;

		rec.oxid = state->oxid;
		for (i = 0; i < (int) UndoLogsCount; i++)
		{
			rec.undoType = (UndoLogType) i;
			rec.undoLocation = state->undo_stacks[i];
			write_to_xids_queue(&rec);
		}

		dlist_foreach(iter, &state->checkpoint_undo_stacks)
		{
			CheckpointUndoStack *stack = dlist_container(CheckpointUndoStack,
														 node,
														 iter.cur);

			rec.undoType = stack->undoType;
			rec.undoLocation = stack->undoStack;
			write_to_xids_queue(&rec);
		}
	}

	if (worker_id < 0)
		recovery_undo_loc_flush->recoveryMainCompletedCheckpointNumber = requestNumber;
	else
		worker_ptrs[worker_id].flushedUndoLocCompletedCheckpointNumber = requestNumber;
}

static void
update_undo_loc_flush_completed_number(bool single)
{
	uint32		completedNumber;

	completedNumber = recovery_undo_loc_flush->recoveryMainCompletedCheckpointNumber;
	if (!single)
	{
		int			i;

		for (i = 0; i < recovery_pool_size_guc; i++)
			completedNumber = Min(completedNumber, worker_ptrs[i].flushedUndoLocCompletedCheckpointNumber);
	}
	recovery_undo_loc_flush->completedCheckpointNumber = completedNumber;
}

/*
 * Handles immediate undo positions flush request from checkpointer.
 */
void
update_recovery_undo_loc_flush(bool single, int worker_id)
{
	uint32		myCompletedNumber,
				requestNumber;

	requestNumber = recovery_undo_loc_flush->immediateRequestCheckpointNumber;
	if (recovery_undo_loc_flush->completedCheckpointNumber >= requestNumber)
		return;

	if (worker_id < 0)
		myCompletedNumber = recovery_undo_loc_flush->recoveryMainCompletedCheckpointNumber;
	else
		myCompletedNumber = worker_ptrs[worker_id].flushedUndoLocCompletedCheckpointNumber;

	/*
	 * Process immediate request if any.
	 */
	if (myCompletedNumber < requestNumber)
		recovery_write_to_xids_queue(worker_id, requestNumber);

	if (worker_id >= 0)
		return;

	update_undo_loc_flush_completed_number(single);
}

void
recovery_on_proc_exit(int code, Datum arg)
{
	int			worker_id = (int) arg;
	uint32		myCompletedNumber,
				requestNumber;
	bool		single = *recovery_single_process;

	if (!recovery_xid_state_hash)
		return;

	elog(LOG, "recovery on exit: %d", worker_id);

	requestNumber = recovery_undo_loc_flush->finishRequestCheckpointNumber;
	if (worker_id < 0)
		myCompletedNumber = recovery_undo_loc_flush->recoveryMainCompletedCheckpointNumber;
	else
		myCompletedNumber = worker_ptrs[worker_id].flushedUndoLocCompletedCheckpointNumber;

	if (recovery_undo_loc_flush->completedCheckpointNumber >= requestNumber)
	{
		requestNumber = checkpoint_state->lastCheckpointNumber + 1;
		before_writing_xids_file(requestNumber);
	}

	/*
	 * Process immediate request if any.
	 */
	if (myCompletedNumber < requestNumber)
		recovery_write_to_xids_queue(worker_id, requestNumber);

	SpinLockAcquire(&recovery_undo_loc_flush->exitLock);
	update_undo_loc_flush_completed_number(single);
	SpinLockRelease(&recovery_undo_loc_flush->exitLock);

	/*
	 * Also write our xids to the last checkpoint caused by fast shutdown.
	 */
	LWLockAcquire(&checkpoint_state->oXidQueueLock, LW_EXCLUSIVE);
	requestNumber = checkpoint_state->lastCheckpointNumber + 1;
	before_writing_xids_file(requestNumber);
	LWLockRelease(&checkpoint_state->oXidQueueLock);

	recovery_write_to_xids_queue(worker_id, requestNumber);
	SpinLockAcquire(&recovery_undo_loc_flush->exitLock);
	update_undo_loc_flush_completed_number(single);
	SpinLockRelease(&recovery_undo_loc_flush->exitLock);
}

static void
o_handle_startup_proc_interrupts_hook(void)
{
	if (is_recovery_in_progress())
		update_proc_retain_undo_location(-1);

	update_recovery_undo_loc_flush(*recovery_single_process, -1);
}

static void
abort_recovery(RecoveryWorkerState *workers_pool, bool send_to_idx_pool)
{
	int			i;
	int			start,
				finish;

	if (send_to_idx_pool)
	{
		Assert(recovery_idx_pool_size_guc);
		start = index_build_first_worker;
		finish = index_build_last_worker;
	}
	else
	{
		start = 0;
		finish = recovery_idx_pool_size_guc ? index_build_leader : recovery_last_worker;
	}

	for (i = start; i <= finish; i++)
	{
		if (workers_pool[i].queue != NULL)
			shm_mq_detach(workers_pool[i].queue);

		if (workers_pool[i].handle != NULL)
		{
			TerminateBackgroundWorker(workers_pool[i].handle);
			worker_wait_shutdown(&workers_pool[i]);
		}
	}

	elog(LOG, "orioledb recovery finished: abort recovery.");
}

/*
 * WaitForBackgroundWorkerShutdown() does not work in this context. We need
 * an analog.
 */
void
worker_wait_shutdown(RecoveryWorkerState *worker)
{
	BgwHandleStatus status;
	pid_t		not_used;

	Assert(worker != NULL);
	Assert(worker->handle != NULL);

	while (true)
	{
		CHECK_FOR_INTERRUPTS();

		status = GetBackgroundWorkerPid(worker->handle, &not_used);

		if (status == BGWH_POSTMASTER_DIED)
			break;
		else if (status == BGWH_STOPPED)
			break;

		pg_usleep(200);
	}
}

static void
cleanup_tablespace_old_files(char *path, uint32 chkp_num, bool before_recovery)
{
	DIR		   *dir,
			   *dbDir;
	struct dirent *file,
			   *dbFile;
	char	   *filename;
	char		ext[5];


	dir = opendir(path);
	if (dir == NULL)
		return;

	while (errno = 0, (file = readdir(dir)) != NULL)
	{
		Oid			dbOid;
		char	   *dbDirName;
		bool		fsyncDbDir = false;

		if (sscanf(file->d_name, "%u", &dbOid) != 1)
			continue;

		dbDirName = psprintf("%s/%u", path, dbOid);

		dbDir = opendir(dbDirName);
		if (dbDir == NULL)
		{
			pfree(dbDirName);
			continue;
		}

		while (errno = 0, (dbFile = readdir(dbDir)) != NULL)
		{
			uint32		file_reloid,
						file_chkp,
						file_segno;
			bool		cleanup = false;

			if (orioledb_s3_mode &&
				(sscanf(dbFile->d_name, "%10u-%10u",
						&file_reloid, &file_chkp) == 2 ||
				 sscanf(dbFile->d_name, "%10u.%10u-%10u",
						&file_reloid, &file_segno, &file_chkp) == 3) &&
				file_chkp > chkp_num)
			{
				cleanup = true;
			}

			if (sscanf(dbFile->d_name, "%10u-%10u.%4s",
					   &file_reloid, &file_chkp, ext) == 3)
			{
				if (before_recovery)
				{
					/*---
					 * Before recovery we should cleanup:
					 *
					 * 1. *.map and *.tmp files which were not created by
					 * checkpointer.
					 * 2. All free extents tree files.
					 *
					 * Otherwise:
					 *
					 * 1. In some cases wrong *.map files will be created.
					 * (if size of old *.map or *.tmp file is more than will
					 * be created by checkpointer).
					 */
					if (!strcmp(ext, "tmp"))
					{
						cleanup = (file_chkp > chkp_num);
					}
					else if (!strcmp(ext, "map"))
					{
						uint32		my_chkp_num;
						bool		found;

						my_chkp_num = o_get_latest_chkp_num(dbOid, file_reloid,
															chkp_num, &found);

						cleanup = (file_chkp > my_chkp_num);

						if (!found && file_chkp == chkp_num)
							o_update_latest_chkp_num(dbOid, file_reloid,
													 file_chkp);
					}

					if (!cleanup)
					{
						ORelOids	oids = {dbOid, file_reloid, file_reloid};

						cleanup = IS_SYS_TREE_OIDS(oids) && sys_tree_get_storage_type(oids.relnode) == BTreeStorageTemporary;
					}
				}
				else
				{
					/*
					 * After recovery we should cleanup old *.tmp and *.map
					 * files.
					 */
					if (!strcmp(ext, "tmp"))
					{
						cleanup = (file_chkp <= chkp_num);
					}
					else if (!strcmp(ext, "map"))
					{
						uint32		my_chkp_num;

						my_chkp_num = o_get_latest_chkp_num(dbOid, file_reloid,
															chkp_num, NULL);

						cleanup = (file_chkp < my_chkp_num);
					}
				}
			}
			else if (before_recovery &&
					 sscanf(dbFile->d_name, "%10u", &file_reloid) == 1)
			{
				/*
				 * Removes free extents tree data files.
				 */
				ORelOids	oids = {dbOid, file_reloid, file_reloid};

				cleanup = IS_SYS_TREE_OIDS(oids) && sys_tree_get_storage_type(oids.relnode) == BTreeStorageTemporary;
			}

			if (cleanup)
			{
				filename = psprintf("%s/%u/%s", path, dbOid, dbFile->d_name);

				if (unlink(filename) < 0)
				{
					ereport(FATAL,
							(errcode_for_file_access(),
							 errmsg("could not remove file \"%s\": %m",
									filename)));
				}
				fsyncDbDir = true;
			}
		}
		closedir(dbDir);
		if (fsyncDbDir)
			fsync_fname_ext(dbDirName, true, false, FATAL);
		pfree(dbDirName);
	}

	if (errno != 0)
	{
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("unable to clean up temporary files: %m")));
	}
	closedir(dir);
}

void
recovery_cleanup_old_files(uint32 chkp_num, bool before_recovery)
{
	DIR		   *dir;
	char		path[MAXPGPATH];
	char		targetpath[MAXPGPATH];
	struct dirent *file;

#define PG_TBLSPC "pg_tblspc"

	if (!before_recovery && chkp_num == 0)
		return;

	path[0] = '\0';
	strlcat(path, ORIOLEDB_DATA_DIR, MAXPGPATH);
	cleanup_tablespace_old_files(path, chkp_num, before_recovery);

	dir = opendir(PG_TBLSPC);
	while (errno = 0, (file = readdir(dir)) != NULL)
	{
		struct stat st;
		int			rllen;

		/* Skip special stuff */
		if (strcmp(file->d_name, ".") == 0 || strcmp(file->d_name, "..") == 0)
			continue;

		path[0] = '\0';
		pg_snprintf(path, MAXPGPATH,
					PG_TBLSPC "/%s/" TABLESPACE_VERSION_DIRECTORY,
					file->d_name);
		if (lstat(path, &st) < 0)
		{
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not stat file \"%s\": %m",
							file->d_name)));
		}

		if (!S_ISLNK(st.st_mode))
		{
			strlcat(path, "/" ORIOLEDB_DATA_DIR, MAXPGPATH);
			cleanup_tablespace_old_files(path, chkp_num, before_recovery);
		}
		else
		{
			rllen = readlink(path, targetpath, sizeof(targetpath));
			if (rllen < 0)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not read symbolic link \"%s\": %m",
								path)));
			if (rllen >= sizeof(targetpath))
				ereport(ERROR,
						(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
						 errmsg("symbolic link \"%s\" target is too long",
								path)));
			targetpath[rllen] = '\0';

			path[0] = '\0';
			pg_snprintf(path, MAXPGPATH,
						"%s/" ORIOLEDB_DATA_DIR,
						targetpath);
			cleanup_tablespace_old_files(path, chkp_num, before_recovery);
		}
	}
	closedir(dir);
#undef PG_TBLSPC
}

static ORelOids *
o_indices_get_oids(Pointer tuple, ORelOids *tableOids)
{
	OIndexChunk chunk;
	ORelOids   *treeOids;

	memcpy(&chunk, tuple, offsetof(OIndexChunk, data));

	if (chunk.key.chunknum != 0)
		return NULL;

	Assert(chunk.dataLength >= sizeof(*tableOids));
	memcpy(tableOids, tuple + offsetof(OIndexChunk, data), sizeof(*tableOids));
	treeOids = (ORelOids *) MemoryContextAlloc(CurTransactionContext, sizeof(ORelOids));
	*treeOids = chunk.key.oids;

	return treeOids;
}

static void
clean_workers_oids(void)
{
	int			i;

	for (i = 0; i < recovery_pool_size_guc; i++)
	{
		RecoveryWorkerState *state = &workers_pool[i];

		state->oids.datoid = InvalidOid;
		state->oids.reloid = InvalidOid;
		state->oids.relnode = InvalidOid;
		state->type = oIndexInvalid;
	}
}

void
recovery_send_leader_oids(ORelOids oids, OIndexNumber ix_num, uint32 o_table_version,
						  ORelOids old_oids, uint32 old_o_table_version,	/* Non-zero only for
																			 * rebuild */
						  bool isrebuild)
{
	RecoveryMsgLeaderIdxBuild msg;
	RecoveryIdxBuildQueueState *state;

	Assert(!(*recovery_single_process));
	Assert(ORelOidsIsValid(oids));

	memset(&msg, 0, sizeof(msg));

	msg.header.type = RecoveryMsgTypeLeaderParallelIndexBuild;
	msg.oids = oids;
	msg.old_oids = old_oids;
	msg.ix_num = ix_num;
	msg.o_table_version = o_table_version;
	msg.old_o_table_version = old_o_table_version;

	Assert(o_tables_get_by_oids_and_version(oids, &o_table_version) != NULL);

	/* Remember oids of index build added to a queue in a hash table */
	state = (RecoveryIdxBuildQueueState *) hash_search(idxbuild_oids_hash,
													   &oids,
													   HASH_ENTER,
													   NULL);

	state->position = pg_atomic_add_fetch_u64(recovery_index_next_pos, 1);
	msg.isrebuild = isrebuild;
	msg.oxid = recovery_oxid;
	msg.current_position = state->position;

	worker_send_msg(index_build_leader, (Pointer) &msg, sizeof(msg));
	worker_queue_flush(index_build_leader);
}

void
recovery_send_worker_oids(dsm_handle seg_handle)
{
	RecoveryMsgWorkerIdxBuild msg;

	Assert(!(*recovery_single_process));

	msg.header.type = RecoveryMsgTypeWorkerParallelIndexBuild;
	msg.oxid = recovery_oxid;
	msg.seg_handle = seg_handle;

	for (int i = index_build_first_worker; i <= index_build_last_worker; i++)
	{
		worker_send_msg(i, (Pointer) &msg, sizeof(msg));
		worker_queue_flush(i);
	}
}

static void
recovery_send_init(int worker_num)
{
	RecoveryMsgEmpty msg;

	Assert(!(*recovery_single_process));

	msg.header.type = RecoveryMsgTypeInit;

	worker_send_msg(worker_num, (Pointer) &msg, sizeof(msg));
	worker_queue_flush(worker_num);
}

static void
handle_o_tables_meta_unlock(ORelOids oids, Oid oldRelnode)
{
	if (!cur_state->o_tables_meta_locked)
	{
		/*
		 * It might happend that we didn't replay WAL_REC_O_TABLES_META_LOCK
		 * wal record.  That means we've finished index build before
		 * checkpoint of a tree was actually started.
		 */
		return;
	}

	if (ORelOidsIsValid(oids))
		recreate_table_descr_by_oids(oids);

	if (reachedConsistency && ORelOidsIsValid(oids))
	{
		OTable	   *new_o_table = NULL;
		OTable	   *old_o_table = NULL;
		OTableDescr *old_descr;
		OIndexNumber ix_num;
		uint16		nindices;

		new_o_table = o_tables_get(oids);
		Assert(new_o_table);

		if (!OidIsValid(oldRelnode))
		{
			uint32		version = new_o_table->version - 1;

			old_o_table = o_tables_get_by_oids_and_version(oids,
														   &version);
		}
		else
		{
			ORelOids	oldOids = {oids.datoid, oids.reloid, oldRelnode};

			old_o_table = o_tables_get(oldOids);
		}
		Assert(old_o_table);


		nindices = Max(old_o_table->nindices, new_o_table->nindices);
		for (ix_num = 0; ix_num < nindices - 1; ix_num++)
		{
			if (!ORelOidsIsEqual(old_o_table->indices[ix_num].oids,
								 new_o_table->indices[ix_num].oids))
				break;
		}

		if (new_o_table->nindices > old_o_table->nindices)
		{
			OTableDescr tmp_descr;

			o_fill_tmp_table_descr(&tmp_descr, new_o_table);
			if (new_o_table->indices[ix_num].type == oIndexPrimary)
			{
				if (tbl_data_exists(&old_o_table->oids))
				{
					old_descr = o_fetch_table_descr(old_o_table->oids);
					rebuild_indices_insert_placeholders(&tmp_descr);
					o_tables_meta_unlock_no_wal();

					Assert(is_recovery_in_progress());

					/*
					 * In main recovery worker send message to main index
					 * creation worker in dedicated recovery workers pool and
					 * exit
					 */
					if (!*recovery_single_process)
					{
						Assert(new_o_table->nindices == nindices);
						/* Send recovery message to become a leader */
						recovery_send_leader_oids(oids, InvalidIndexNumber,
												  new_o_table->version,
												  old_o_table->oids,
												  old_o_table->version,
												  true);
					}
					else
						rebuild_indices(old_o_table, old_descr,
										new_o_table, &tmp_descr, false, NULL);
				}
				else
				{
					o_tables_meta_unlock_no_wal();
				}
			}
			else
			{

				o_insert_shared_root_placeholder(new_o_table->indices[ix_num].oids.datoid,
												 new_o_table->indices[ix_num].oids.relnode);
				o_tables_meta_unlock_no_wal();

				Assert(is_recovery_in_progress());

				/*
				 * In main recovery worker send message to main index creation
				 * worker in dedicated recovery workers pool and exit
				 */
				if (!*recovery_single_process)
				{
					ORelOids	invalid_oids;

					Assert(new_o_table->nindices == nindices);
					/* Send recovery message to become a leader */
					ORelOidsSetInvalid(invalid_oids);
					recovery_send_leader_oids(oids, ix_num, new_o_table->version,
											  invalid_oids, 0, false);
				}
				else
					build_secondary_index(new_o_table, &tmp_descr, ix_num, false, false, NULL);
			}
			o_free_tmp_table_descr(&tmp_descr);
		}
		else if (new_o_table->nindices < old_o_table->nindices)
		{
			if (old_o_table->indices[ix_num].type == oIndexPrimary)
			{
				OTableDescr tmp_descr;

				o_fill_tmp_table_descr(&tmp_descr, new_o_table);
				if (tbl_data_exists(&old_o_table->indices[ix_num].oids))
				{
					old_descr = o_fetch_table_descr(old_o_table->oids);
					rebuild_indices_insert_placeholders(&tmp_descr);
					o_tables_meta_unlock_no_wal();

					/*
					 * In main recovery worker send message to main index
					 * creation worker in dedicated recovery workers pool and
					 * exit
					 */
					if (!*recovery_single_process)
					{
						/* Send recovery message to become a leader */
						recovery_send_leader_oids(oids, InvalidIndexNumber,
												  new_o_table->version,
												  old_o_table->oids,
												  old_o_table->version,
												  true);
					}
					else
						rebuild_indices(old_o_table, old_descr,
										new_o_table, &tmp_descr, false, NULL);
				}
				else
				{
					o_tables_meta_unlock_no_wal();
				}
				o_free_tmp_table_descr(&tmp_descr);
			}
			else
			{
				o_tables_meta_unlock_no_wal();
			}
		}
		else
		{
			o_tables_meta_unlock_no_wal();
		}

		pfree(old_o_table);
		pfree(new_o_table);
	}
	else
	{
		o_tables_meta_unlock_no_wal();
	}

	cur_state->o_tables_meta_locked = false;
}

static void
invalidate_typcache(void)
{
	SharedInvalidationMessage msg;

	msg.cc.id = TYPEOID;
	msg.cc.dbId = InvalidOid;
	msg.cc.hashValue = 0;

	/*
	 * check AddCatcacheInvalidationMessage() for an explanation
	 */
	VALGRIND_MAKE_MEM_DEFINED(&msg, sizeof(msg));

	SendSharedInvalidMessages(&msg, 1);
}

/*
 * Read tuples from modify WAL record.
 * tuple1 is mandatory basic tuple for all modify records,
 * tuple2 is optional, e.g. WAL record could also have old tuple in case of REINSERT
 */
void
read_modify_wal_tuples(uint8 rec_type, Pointer *ptr, OFixedTuple *tuple1, OFixedTuple *tuple2, OffsetNumber *length1_out)
{
	bool		contains_two_tuples = (rec_type == WAL_REC_REINSERT);
	OffsetNumber length1;
	OffsetNumber length2;

	Assert(rec_type == WAL_REC_INSERT || rec_type == WAL_REC_UPDATE || rec_type == WAL_REC_DELETE || rec_type == WAL_REC_REINSERT);

	if(!contains_two_tuples)
	{
		tuple1->tuple.formatFlags = **ptr;
		(*ptr)++;

		memcpy(&length1, *ptr, sizeof(OffsetNumber));
		*ptr += sizeof(OffsetNumber);

		Assert(length1 > 0);
		if (length1 != 0)
		{
			memcpy(tuple1->fixedData, *ptr, length1);
			*ptr += length1;

			tuple1->tuple.data = tuple1->fixedData;
		}
		else
			O_TUPLE_SET_NULL(tuple1->tuple);
	}
	else
	{	
		tuple1->tuple.formatFlags = **ptr;
		(*ptr)++;
		tuple2->tuple.formatFlags = **ptr;
		(*ptr)++;

		memcpy(&length1, *ptr, sizeof(OffsetNumber));
		*ptr += sizeof(OffsetNumber);
		memcpy(&length2, *ptr, sizeof(OffsetNumber));
		*ptr += sizeof(OffsetNumber);

		Assert(length1 > 0);
		if (length1 != 0)
		{
			memcpy(tuple1->fixedData, *ptr, length1);
			*ptr += length1;

			tuple1->tuple.data = tuple1->fixedData;
		}
		else
			O_TUPLE_SET_NULL(tuple1->tuple);

		Assert(length2 > 0);
		if (length2 != 0)
		{
			memcpy(tuple2->fixedData, *ptr, length2);
			*ptr += length2;

			tuple2->tuple.data = tuple2->fixedData;
		}
		else
			O_TUPLE_SET_NULL(tuple2->tuple);
	}
	*length1_out = length1;
}

/*
 * Replays a single orioledb WAL container.
 */
static bool
replay_container(Pointer startPtr, Pointer endPtr,
				 bool single, XLogRecPtr xlogRecPtr, XLogRecPtr xlogRecEndPtr)
{
	OTableDescr *descr = NULL;
	OIndexDescr *indexDescr = NULL;
	OXid		oxid = InvalidOXid;
	ORelOids	cur_oids = {0, 0, 0},
			   *treeOids;
	OffsetNumber unused;
	bool		success;
	uint16		type;
	uint8		rec_type;
	int			sys_tree_num = -1;
	Pointer		ptr = startPtr;
	XLogRecPtr	xlogPtr;
	uint16		wal_version;

	wal_version = check_wal_container_version(&ptr);
	if (wal_version > CURRENT_WAL_VERSION)
	{
		/* WAL from future version */
		return false;
	}

	while (ptr < endPtr)
	{
		xlogPtr = xlogRecPtr + (ptr - startPtr);
		rec_type = *ptr;
		ptr++;

		if (rec_type == WAL_REC_XID)
		{
			memcpy(&oxid, ptr, sizeof(oxid));
			ptr += sizeof(oxid);

			/* don't need logicalXid here */
			ptr += sizeof(TransactionId);

			advance_oxids(oxid);
			recovery_switch_to_oxid(oxid, -1);
		}
		else if (rec_type == WAL_REC_COMMIT || rec_type == WAL_REC_ROLLBACK)
		{
			bool		commit,
						sync = false;
			OXid		xmin;

			memcpy(&xmin, ptr, sizeof(xmin));
			ptr += sizeof(xmin);

			/* skip csn field */
			ptr += sizeof(CommitSeqNo);

			recovery_xmin = Max(recovery_xmin, xmin);

			Assert(sys_tree_num <= 0 || sys_tree_supports_transactions(sys_tree_num));

			commit = (rec_type == WAL_REC_COMMIT);

			Assert(oxid != InvalidOXid);

			if (!single)
			{
				Assert(cur_state != NULL);
				workers_send_oxid_finish(xlogRecEndPtr, commit);
				if (cur_state->systree_modified || cur_state->checkpoint_xid)
				{
					sync = true;
					workers_synchronize(xlogPtr, false);
					if (cur_state->invalidate_typcache)
						invalidate_typcache();
				}
			}
			else
			{
				sync = true;
				pg_atomic_write_u64(recovery_ptr, xlogPtr);
				if (cur_state->invalidate_typcache)
					invalidate_typcache();

			}

			recovery_finish_current_oxid(commit ? COMMITSEQNO_MAX_NORMAL - 1 : COMMITSEQNO_ABORTED,
										 xlogPtr, -1, sync);
			oxid = InvalidOXid;
		}
		else if (rec_type == WAL_REC_JOINT_COMMIT)
		{
			TransactionId xid;
			OXid		xmin;

			memcpy(&xid, ptr, sizeof(xid));
			ptr += sizeof(xid);
			memcpy(&xmin, ptr, sizeof(xmin));
			ptr += sizeof(xmin);

			/* skip csn field */
			ptr += sizeof(CommitSeqNo);

			cur_state->xid = xid;
			recovery_xmin = Max(recovery_xmin, xmin);
			dlist_push_tail(&joint_commit_list, &cur_state->joint_commit_list_node);
		}
		else if (rec_type == WAL_REC_RELATION)
		{
			OIndexType	ix_type;

			ix_type = *ptr;
			ptr++;
			memcpy(&cur_oids.datoid, ptr, sizeof(Oid));
			ptr += sizeof(Oid);
			memcpy(&cur_oids.reloid, ptr, sizeof(Oid));
			ptr += sizeof(Oid);
			memcpy(&cur_oids.relnode, ptr, sizeof(Oid));
			ptr += sizeof(Oid);

			if (IS_SYS_TREE_OIDS(cur_oids))
				sys_tree_num = cur_oids.relnode;
			else
				sys_tree_num = -1;

			if (sys_tree_num > 0)
			{
				descr = NULL;
				indexDescr = NULL;
				Assert(sys_tree_get_storage_type(sys_tree_num) == BTreeStoragePersistence);
			}
			else if (ix_type == oIndexInvalid)
			{
				descr = o_fetch_table_descr(cur_oids);
				indexDescr = descr ? GET_PRIMARY(descr) : NULL;
			}
			else
			{
				Assert(ix_type == oIndexToast || ix_type == oIndexBridge);
				descr = NULL;
				indexDescr = o_fetch_index_descr(cur_oids, ix_type,
												 false, NULL);
			}

			if (sys_tree_num == -1)
			{
				char	   *prefix;
				char	   *db_prefix;

				o_get_prefixes_for_relnode(cur_oids.datoid, cur_oids.relnode,
										   &prefix, &db_prefix);
				o_verify_dir_exists_or_create(prefix, NULL, NULL);
				o_verify_dir_exists_or_create(db_prefix, NULL, NULL);
				pfree(db_prefix);
			}
		}
		else if (rec_type == WAL_REC_O_TABLES_META_LOCK)
		{
			Assert(!cur_state->o_tables_meta_locked);
			o_tables_meta_lock_no_wal();
			cur_state->o_tables_meta_locked = true;
		}
		else if (rec_type == WAL_REC_O_TABLES_META_UNLOCK)
		{
			ORelOids	oids;
			Oid			oldRelnode;

			memcpy(&oids.datoid, ptr, sizeof(Oid));
			ptr += sizeof(Oid);
			memcpy(&oids.reloid, ptr, sizeof(Oid));
			ptr += sizeof(Oid);
			memcpy(&oldRelnode, ptr, sizeof(Oid));
			ptr += sizeof(Oid);
			memcpy(&oids.relnode, ptr, sizeof(Oid));
			ptr += sizeof(Oid);

			if (!single)
				workers_synchronize(xlogPtr, true);

			Assert(cur_state->o_tables_meta_locked);
			handle_o_tables_meta_unlock(oids, oldRelnode);

			if (!single)
				workers_synchronize(xlogPtr + 1, true);

			if (!single)
				clean_workers_oids();
		}
		else if (rec_type == WAL_REC_TRUNCATE)
		{
			ORelOids	oids;

			memcpy(&oids.datoid, ptr, sizeof(Oid));
			ptr += sizeof(Oid);
			memcpy(&oids.reloid, ptr, sizeof(Oid));
			ptr += sizeof(Oid);
			memcpy(&oids.relnode, ptr, sizeof(Oid));
			ptr += sizeof(Oid);

			if (!single)
				workers_synchronize(xlogPtr, true);

			o_truncate_table(oids);

			AcceptInvalidationMessages();
			if (!single)
				clean_workers_oids();
		}
		else if (rec_type == WAL_REC_SAVEPOINT)
		{
			SubTransactionId parentSubid;

			memcpy(&parentSubid, ptr, sizeof(SubTransactionId));
			ptr += sizeof(SubTransactionId);
			ptr += 2 * sizeof(TransactionId);

			recovery_savepoint(parentSubid, -1);

			if (!single)
				workers_send_savepoint(parentSubid);
		}
		else if (rec_type == WAL_REC_ROLLBACK_TO_SAVEPOINT)
		{
			SubTransactionId parentSubid;

			memcpy(&parentSubid, ptr, sizeof(SubTransactionId));
			ptr += sizeof(SubTransactionId);

			if (!single)
			{
				workers_send_rollback_to_savepoint(xlogPtr, parentSubid);
				workers_synchronize(xlogPtr, false);
			}
			recovery_rollback_to_savepoint(parentSubid, -1);
		}
		else if (rec_type == WAL_REC_BRIDGE_ERASE)
		{
			ItemPointerData iptr;

			memcpy(&iptr, ptr, sizeof(iptr));
			ptr += sizeof(iptr);

			Assert(indexDescr);

			if (single)
			{
				recovery_switch_to_oxid(oxid, -1);
				replay_erase_bridge_item(indexDescr, &iptr);
			}
			else
			{
				uint32		hash;
				OTuple		tuple;

				hash = o_hash_iptr(indexDescr, &iptr);
				tuple.formatFlags = 0;
				tuple.data = (Pointer) &iptr;
				worker_send_modify(GET_WORKER_ID(hash), &indexDescr->desc,
								   RecoveryMsgTypeBridgeErase, tuple, 0);
			}
		}
		else if (rec_type == WAL_REC_INSERT || rec_type == WAL_REC_UPDATE || rec_type == WAL_REC_DELETE || rec_type == WAL_REC_REINSERT)
		{
			OFixedTuple tuple1;
			OFixedTuple tuple2;
			Pointer		sys_tree_oids_ptr;

			sys_tree_oids_ptr = ptr + sizeof(uint8) + sizeof(OffsetNumber);

			read_modify_wal_tuples(rec_type, &ptr, &tuple1, &tuple2, &unused);

			type = recovery_msg_from_wal_record(rec_type);

			Assert(oxid != InvalidOXid);

			if (sys_tree_num > 0 && xlogRecPtr >= checkpoint_state->sysTreesStartPtr)
			{
				Assert(sys_tree_supports_transactions(sys_tree_num));
				recovery_switch_to_oxid(oxid, -1);

				cur_state->systree_modified = true;
				if (IS_TYPCACHE_SYSTREE(sys_tree_num))
					cur_state->invalidate_typcache = true;
				if (sys_tree_num == SYS_TREES_O_TABLES)
					Assert(cur_state->o_tables_meta_locked);

				if (!single)
					workers_synchronize(xlogPtr, true);

				success = apply_sys_tree_modify_record(sys_tree_num, type,
													   tuple1.tuple, oxid,
													   COMMITSEQNO_INPROGRESS);

				if (sys_tree_num == SYS_TREES_O_INDICES && success)
				{
					ORelOids	tmp_oids;

					if (type == RecoveryMsgTypeDelete)
					{
						treeOids = o_indices_get_oids(sys_tree_oids_ptr, &tmp_oids);
						if (treeOids)
							add_undo_drop_relnode(tmp_oids, treeOids, 1);
					}
					else if (type == RecoveryMsgTypeInsert)
					{
						treeOids = o_indices_get_oids(sys_tree_oids_ptr, &tmp_oids);
						if (treeOids)
							add_undo_create_relnode(tmp_oids, treeOids, 1, true);
					}
				}
				else if (sys_tree_num == SYS_TREES_TABLESPACE_CACHE && success)
				{
					OSysCacheKey1 key;
					char	   *prefix;
					char	   *db_prefix;

					memcpy(&key, sys_tree_oids_ptr, sizeof(OSysCacheKey1));
					o_get_prefixes_for_relnode(key.common.datoid, DatumGetObjectId(key.keys[0]),
											   &prefix, &db_prefix);
					o_verify_dir_exists_or_create(prefix, NULL, NULL);
					o_verify_dir_exists_or_create(db_prefix, NULL, NULL);
					pfree(db_prefix);
				}
			}

			if (sys_tree_num > 0 || indexDescr == NULL)
			{
				/* nothing to do here */
				continue;
			}

			if (indexDescr->desc.type == oIndexBridge)
			{
				elog(LOG, "WAL change for bridge index");
			}

			Assert(!O_TUPLE_IS_NULL(tuple1.tuple));

			/* Reinsert is processed as DELETE + INSERT */
			if (rec_type == WAL_REC_REINSERT)
			{
				Assert(type == RecoveryMsgTypeReinsert);
				Assert(!O_TUPLE_IS_NULL(tuple2.tuple));

				if (single)
				{
					recovery_switch_to_oxid(oxid, -1);
					apply_modify_record(descr, indexDescr, RecoveryMsgTypeDelete, tuple2.tuple);
					apply_modify_record(descr, indexDescr, RecoveryMsgTypeInsert, tuple1.tuple);
				}
				else
				{
					spread_idx_modify(&indexDescr->desc, RecoveryMsgTypeDelete, tuple2.tuple);
					spread_idx_modify(&indexDescr->desc, RecoveryMsgTypeInsert, tuple1.tuple);
				}
			}
			else
			{
				if (single)
				{
					recovery_switch_to_oxid(oxid, -1);
					apply_modify_record(descr, indexDescr, type, tuple1.tuple);
				}
				else
				{
					spread_idx_modify(&indexDescr->desc, type, tuple1.tuple);
				}
			}
		}
		else
		{
			elog(FATAL, "Unknown modify WAL record");
		}
	}
	update_recovery_undo_loc_flush(single, -1);
	return true;
}

/*
 * Hook for replaying builtin commit record.  Performs joint commit.
 */
void
o_xact_redo_hook(TransactionId xid, XLogRecPtr lsn)
{
	dlist_mutable_iter miter;
	bool		single = *recovery_single_process;

	dlist_foreach_modify(miter, &joint_commit_list)
	{
		RecoveryXidState *state;
		bool		sync = false;

		state = dlist_container(RecoveryXidState, joint_commit_list_node, miter.cur);
		if (state->xid != xid)
			continue;

		recovery_switch_to_oxid(state->oxid, -1);

		if (!single)
		{
			Assert(cur_state != NULL);
			workers_send_oxid_finish(lsn, true);
			if (cur_state->systree_modified || cur_state->checkpoint_xid)
			{
				sync = true;
				workers_synchronize(lsn, false);
				if (cur_state->invalidate_typcache)
					invalidate_typcache();
			}
		}
		else
		{
			sync = true;
			pg_atomic_write_u64(recovery_ptr, lsn);
			if (cur_state->invalidate_typcache)
				invalidate_typcache();
		}

		dlist_delete(miter.cur);
		recovery_finish_current_oxid(COMMITSEQNO_MAX_NORMAL - 1,
									 lsn, -1, sync);
		break;
	}
}

/*
 * Sends the message to a worker.
 */
void
worker_send_msg(int worker_id, Pointer msg, uint64 msg_size)
{
	RecoveryWorkerState *state = &workers_pool[worker_id];

	Assert(workers_pool);
	Assert(state);
	Assert(state->handle);
	if ((RECOVERY_QUEUE_BUF_SIZE - state->queue_buf_len) < msg_size)
		worker_queue_flush(worker_id);

	memcpy(state->queue_buf + state->queue_buf_len, msg, msg_size);
	state->queue_buf_len += msg_size;
}

static void
delay_if_queued_for_idxbuild(void)
{
	while (idxbuild_oids_hash)
	{
		HASH_SEQ_STATUS hash_seq;
		RecoveryIdxBuildQueueState *cur;

		/*
		 * This function might be called by a startup process and by a
		 * recovery worker, therefore check in which worker we are.
		 */
		if (AmStartupProcess())
			HandleStartupProcInterrupts();
		else
			o_worker_handle_interrupts();

		/* Remove hash entries for completed indexes */
		hash_seq_init(&hash_seq, idxbuild_oids_hash);
		while ((cur = (RecoveryIdxBuildQueueState *) hash_seq_search(&hash_seq)) != NULL)
		{
			if (cur->position <= pg_atomic_read_u64(recovery_index_completed_pos))
				hash_search(idxbuild_oids_hash, &cur->oids, HASH_REMOVE, NULL);
		}

		/* All completed ? */
		if (hash_get_num_entries(idxbuild_oids_hash) == 0)
			break;

		/*
		 * We wait on a condition variable that will wake us as soon as the
		 * pause ends, but we use a timeout so we can check the
		 * HandleStartupProcInterrupts() periodically too.
		 */
		ConditionVariableTimedSleep(recovery_index_cv, 1000,
									WAIT_EVENT_PARALLEL_CREATE_INDEX_SCAN);
	}
	ConditionVariableCancelSleep();
}

static void
delay_rels_queued_for_idxbuild(ORelOids oids)
{
	RecoveryIdxBuildQueueState *hash_elem;
	bool		found;

	/*
	 * Delay modify requests if indexes for the relation are requested to be
	 * build but haven't been built yet
	 */
	while (true)
	{
		/*
		 * This function might be called by a startup process and by a
		 * recovery worker, therefore check in which worker we are.
		 */
		if (AmStartupProcess())
			HandleStartupProcInterrupts();
		else
			o_worker_handle_interrupts();

		hash_elem = (RecoveryIdxBuildQueueState *) hash_search(idxbuild_oids_hash,
															   &oids,
															   HASH_FIND,
															   &found);
		if (!found)
		{
			ConditionVariableBroadcast(recovery_index_cv);
			break;
		}

		if (hash_elem->position <= pg_atomic_read_u64(recovery_index_completed_pos))
		{
			/* Remove completed index build and repeat hash search */
			hash_elem = (RecoveryIdxBuildQueueState *) hash_search(idxbuild_oids_hash,
																   &oids,
																   HASH_REMOVE,
																   &found);
		}
		else
		{
			/*
			 * We wait on a condition variable that will wake us as soon as
			 * the pause ends, but we use a timeout so we can check the
			 * HandleStartupProcInterrupts() periodically too.
			 */
			ConditionVariableTimedSleep(recovery_index_cv, 1000,
										WAIT_EVENT_PARALLEL_CREATE_INDEX_SCAN);
		}
	}
	ConditionVariableCancelSleep();
}

/*
 * Sends modify message to a worker.
 */
static void
worker_send_modify(int worker_id, BTreeDescr *desc,
				   RecoveryMsgType recType,
				   OTuple tuple, int tuple_len)
{
	RecoveryMsgHeader *header;
	RecoveryWorkerState *state = &workers_pool[worker_id];
	Pointer		data;
	int			max_msg_size;
	ORelOids	oids;
	OIndexType	type;

	if (!IS_SYS_TREE_OIDS(desc->oids))
	{
		if (desc->type == oIndexPrimary)
		{
			OIndexDescr *id = (OIndexDescr *) desc->arg;

			oids = id->tableOids;
			type = oIndexInvalid;
		}
		else
		{
			Assert(desc->type == oIndexToast || desc->type == oIndexBridge);
			oids = desc->oids;
			type = desc->type;
		}
	}
	else
	{
		oids = desc->oids;
		type = oIndexPrimary;
		Assert(desc->type == oIndexPrimary);
	}

	delay_rels_queued_for_idxbuild(oids);

	max_msg_size = MAXALIGN(sizeof(RecoveryMsgHeader) + sizeof(OXid)
							+ sizeof(ORelOids) + 1
							+ sizeof(int) + 1) + tuple_len;

	Assert(recType == RecoveryMsgTypeInsert ||
		   recType == RecoveryMsgTypeUpdate ||
		   recType == RecoveryMsgTypeDelete ||
		   recType == RecoveryMsgTypeBridgeErase);

	if (RECOVERY_QUEUE_BUF_SIZE - state->queue_buf_len < max_msg_size)
		worker_queue_flush(worker_id);

	data = state->queue_buf + state->queue_buf_len;
	header = (RecoveryMsgHeader *) data;
	header->type = recType;
	data += sizeof(RecoveryMsgHeader);
	state->queue_buf_len += sizeof(RecoveryMsgHeader);

	Assert(cur_state || recType == RecoveryMsgTypeBridgeErase);
	if (recType != RecoveryMsgTypeBridgeErase && state->oxid != cur_state->oxid)
	{
		memcpy(data, &cur_state->oxid, sizeof(OXid));
		data += sizeof(OXid);
		state->queue_buf_len += sizeof(OXid);
		header->type |= RECOVERY_MODIFY_OXID;
		state->oxid = cur_state->oxid;
		cur_state->used_by[worker_id] = true;
	}

	if (!ORelOidsIsEqual(state->oids, oids) || state->type != type)
	{
		memcpy(data, &oids, sizeof(ORelOids));
		data += sizeof(ORelOids);
		*data = type;
		data++;
		state->queue_buf_len += sizeof(ORelOids) + 1;
		header->type |= RECOVERY_MODIFY_OIDS;
		state->oids = oids;
		state->type = type;
	}

	if (recType != RecoveryMsgTypeBridgeErase)
	{
		memcpy(data, &tuple_len, sizeof(int));
		data += sizeof(int);
		memcpy(data, &tuple.formatFlags, 1);

		state->queue_buf_len += sizeof(int) + 1;
		state->queue_buf_len = MAXALIGN(state->queue_buf_len);
		data = state->queue_buf + state->queue_buf_len;

		memcpy(data, tuple.data, tuple_len);
		state->queue_buf_len += tuple_len;
		state->queue_buf_len = MAXALIGN(state->queue_buf_len);
	}
	else
	{
		memcpy(data, tuple.data, sizeof(ItemPointerData));
		state->queue_buf_len += sizeof(ItemPointerData);
		state->queue_buf_len = MAXALIGN(state->queue_buf_len);
	}
}

/*
 * Sends recovery finish message to all workers in the pool.
 */
void
workers_send_finish(bool send_to_idx_pool)
{
	RecoveryMsgEmpty finish_msg;
	RecoveryWorkerState *state;
	int			i;
	int			start,
				finish;

	if (send_to_idx_pool)
	{
		Assert(recovery_idx_pool_size_guc);
		start = index_build_first_worker;
		finish = index_build_last_worker;
	}
	else
	{
		start = 0;
		finish = recovery_idx_pool_size_guc ? index_build_leader : recovery_last_worker;
	}

	for (i = start; i <= finish; i++)
	{
		state = &workers_pool[i];

		finish_msg.header.type = RecoveryMsgTypeFinished;
		if (RECOVERY_QUEUE_BUF_SIZE - state->queue_buf_len < sizeof(RecoveryMsgEmpty))
			worker_queue_flush(i);

		memcpy(state->queue_buf + state->queue_buf_len, &finish_msg, sizeof(RecoveryMsgEmpty));
		state->queue_buf_len += sizeof(RecoveryMsgEmpty);
		worker_queue_flush(i);
	}
}

/*
 * Sends savepoint message to workers with active the oxid in the pool.
 */
static void
workers_send_savepoint(SubTransactionId parentSubId)
{
	RecoveryWorkerState *state;
	RecoveryMsgSavepoint msg;
	int			i;

	Assert(cur_state);

	msg.header.type = RecoveryMsgTypeSavepoint;
	msg.oxid = cur_state->oxid;
	msg.parentSubId = parentSubId;

	for (i = 0; i < recovery_pool_size_guc; i++)
	{
		if (cur_state->used_by[i])
		{
			state = &workers_pool[i];
			if (state->oxid == cur_state->oxid)
				state->oxid = InvalidOXid;

			worker_send_msg(i, (Pointer) &msg, sizeof(msg));

			if (EnableHotStandby)
			{
				/* we need to apply recovery records as fast as we can */
				worker_queue_flush(i);
			}
		}
	}
}

/*
 * Sends rollback to savepoint message to workers with active the oxid in the pool.
 */
static void
workers_send_rollback_to_savepoint(XLogRecPtr ptr,
								   SubTransactionId parentSubId)
{
	RecoveryWorkerState *state;
	RecoveryMsgRollbackToSavepoint msg;
	int			i;

	Assert(cur_state);

	msg.header.type = RecoveryMsgTypeRollbackToSavepointt;
	msg.oxid = cur_state->oxid;
	msg.ptr = ptr;
	msg.parentSubId = parentSubId;

	for (i = 0; i < recovery_pool_size_guc; i++)
	{
		if (cur_state->used_by[i])
		{
			state = &workers_pool[i];
			if (state->oxid == cur_state->oxid)
				state->oxid = InvalidOXid;

			worker_send_msg(i, (Pointer) &msg, sizeof(msg));

			if (EnableHotStandby)
			{
				/* we need to apply recovery records as fast as we can */
				worker_queue_flush(i);
			}
		}
	}
	pg_atomic_write_u64(recovery_ptr, ptr);
}

/*
 * Sends commit or rollback message to workers with active the oxid in the pool.
 */
static void
workers_send_oxid_finish(XLogRecPtr ptr, bool commit)
{
	RecoveryWorkerState *state;
	RecoveryMsgOXidPtr oxid_ptr_record;
	int			i;

	oxid_ptr_record.header.type = commit ? RecoveryMsgTypeCommit : RecoveryMsgTypeRollback;
	oxid_ptr_record.oxid = cur_state->oxid;
	oxid_ptr_record.ptr = ptr;

	for (i = 0; i < recovery_pool_size_guc; i++)
	{
		/*
		 * Notify workers who participated in the current transaction.  For
		 * transactions that participated in the checkpoint xids file, we
		 * notify them by phone because all the works read the xids file and
		 * need to update their local hashes.
		 */
		if (cur_state->used_by[i] || cur_state->checkpoint_xid)
		{
			state = &workers_pool[i];
			if (state->oxid == cur_state->oxid)
				state->oxid = InvalidOXid;

			worker_send_msg(i, (Pointer) &oxid_ptr_record, sizeof(oxid_ptr_record));

			if (EnableHotStandby)
			{
				/* we need to apply recovery records as fast as we can */
				worker_queue_flush(i);
			}
		}
	}
	pg_atomic_write_u64(recovery_ptr, ptr);
}

/*
 * Synchronize execution with workers.
 *
 * Actually used only before delete a relnode. We can hold a list of relnodes
 * used by workers and synchronize only with needed workers. But we assume that
 * it does not happen too often and we can use this simple solution.
 */
static void
workers_synchronize(XLogRecPtr ptr, bool send_synchronize)
{
	int			i;

	if (send_synchronize)
	{
		RecoveryMsgPtr sync_msg;

		sync_msg.header.type = RecoveryMsgTypeSynchronize;
		sync_msg.ptr = ptr;
		for (i = 0; i < recovery_pool_size_guc; i++)
		{
			worker_send_msg(i, (Pointer) &sync_msg, sizeof(sync_msg));
			worker_queue_flush(i);
		}
		pg_atomic_write_u64(recovery_ptr, ptr);
	}

	for (i = 0; i < recovery_pool_size_guc && !unexpected_worker_detach; i++)
	{
		int			j = 0;

		while (pg_atomic_read_u64(&worker_ptrs[i].commitPtr) < ptr &&
			   workers_pool[i].queue)
		{
			BgwHandleStatus status;
			pid_t		pid;

			pg_usleep(10);

			if (j % 100 == 0)
			{
				status = GetBackgroundWorkerPid(workers_pool[i].handle, &pid);
				if (status != BGWH_STARTED && status != BGWH_NOT_YET_STARTED)
				{
					unexpected_worker_detach = true;
					break;
				}
			}
			j++;
		}
	}
}


/*
 * Notify workers that toast reached consistent state.
 */
static void
workers_notify_toast_consistent(void)
{
	RecoveryMsgEmpty msg;
	int			i;

	msg.header.type = RecoveryMsgTypeToastConsistent;

	for (i = 0; i < recovery_pool_size_guc; i++)
	{
		worker_send_msg(i, (Pointer) &msg, sizeof(msg));
		worker_queue_flush(i);
	}
}

/*
 * Flushes a queue buffer to the queue.
 */
void
worker_queue_flush(int worker_id)
{
	RecoveryWorkerState *state = &workers_pool[worker_id];
	shm_mq_result result;

	result = shm_mq_send(state->queue, state->queue_buf_len, state->queue_buf, false, true);
	state->queue_buf_len = 0;
	Assert(result != SHM_MQ_WOULD_BLOCK);
	if (result == SHM_MQ_DETACHED)
	{
		unexpected_worker_detach = true;
		return;
	}
	Assert(result == SHM_MQ_SUCCESS);
}

/*
 * Applies recovery record to o_tables.
 *
 * We do it by master process to avoid concurrent issues such as:
 *
 * Worker can not fetch table description because another worker does not
 * commit transaction yet.
 */
static bool
apply_sys_tree_modify_record(int sys_tree_num, uint16 type, OTuple tup,
							 OXid oxid, CommitSeqNo csn)
{
	bool		result;

	result = apply_btree_modify_record(get_sys_tree(sys_tree_num),
									   type, tup, oxid, csn);

	return result;
}

/*
 * Spreads the index modify recovery record to the recovery workers pool.
 *
 * Tuples with a same key will be processed by a same worker. This approach
 * helps to apply recovery records for tuples in the right order.
 */
static inline void
spread_idx_modify(BTreeDescr *desc, RecoveryMsgType recType, OTuple rec)
{
	OTuple		key PG_USED_FOR_ASSERTS_ONLY;
	uint32		hash;
	int			key_len,
				tup_len;
	bool		key_pfree PG_USED_FOR_ASSERTS_ONLY;

	switch (recType)
	{
		case RecoveryMsgTypeInsert:
		case RecoveryMsgTypeUpdate:
			tup_len = o_btree_len(desc, rec, OTupleLength);
			hash = o_btree_hash(desc, rec, BTreeKeyLeafTuple);
#ifdef USE_ASSERT_CHECKING
			key = o_btree_tuple_make_key(desc, rec, NULL, true, &key_pfree);
			Assert(hash == o_btree_hash(desc, key, BTreeKeyNonLeafKey));
			if (key_pfree)
				pfree(key.data);
#endif
			worker_send_modify(GET_WORKER_ID(hash), desc,
							   recType, rec, tup_len);
			break;
		case RecoveryMsgTypeDelete:
			key_len = o_btree_len(desc, rec, OKeyLength);
			hash = o_btree_hash(desc, rec, BTreeKeyNonLeafKey);
			worker_send_modify(GET_WORKER_ID(hash), desc, recType,
							   rec, key_len);
			break;
		default:
			Assert(false);
	}
}

/*
 * Converts wal record type to recovery message type.
 */
static inline RecoveryMsgType
recovery_msg_from_wal_record(uint8 wal_record)
{
	switch (wal_record)
	{
		case WAL_REC_INSERT:
			return RecoveryMsgTypeInsert;
		case WAL_REC_DELETE:
			return RecoveryMsgTypeDelete;
		case WAL_REC_UPDATE:
			return RecoveryMsgTypeUpdate;
		case WAL_REC_BRIDGE_ERASE:
			return RecoveryMsgTypeBridgeErase;
		case WAL_REC_REINSERT:

			/*
			 * Temporary one for convenience. Splits down to
			 * RecoveryMsgTypeInsert + RecoveryMsgTypeDelete
			 */
			return RecoveryMsgTypeReinsert;
		default:
			Assert(false);
			elog(ERROR, "Wrong WAL record modify type %d", wal_record);
	}
	return (uint16) 0;			/* keep compiler quiet */
}
