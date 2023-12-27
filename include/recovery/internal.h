/*-------------------------------------------------------------------------
 *
 * internal.h
 *		Internal declarations for orioledb engine recovery.
 *
 * Copyright (c) 2021-2023, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/recovery/internal.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __RECOVERY_INTERNAL_H__
#define __RECOVERY_INTERNAL_H__

#include "postgres.h"

#include "orioledb.h"

#include "postmaster/bgworker.h"

/*
 * Recovery transaction support functions.
 */
extern void recovery_init(int worker_id);
extern void recovery_switch_to_oxid(OXid oxid, int worker_id);
extern void recovery_finish_current_oxid(CommitSeqNo csn, XLogRecPtr ptr,
										 int worker_id, bool sync);
extern void recovery_savepoint(SubTransactionId parentSubid, int worker_id);
extern void recovery_rollback_to_savepoint(SubTransactionId parentSubid, int worker_id);
extern void recovery_finish(int worker_id);
extern void update_recovery_undo_loc_flush(bool single, int worker_id);
extern void recovery_on_proc_exit(int code, Datum arg);

extern Pointer recovery_first_queue;
extern uint64 recovery_queue_data_size;

#define GET_WORKER_QUEUE(worker_id) ((void*)(recovery_first_queue \
										+ recovery_queue_data_size * (worker_id)))
#define GET_WORKER_ID(hash) ((hash) % recovery_pool_size_guc)

/*
 * Recovery from master to workers messages format.
 */
#define RECOVERY_INSERT ((uint16) 1 << 0)
#define RECOVERY_DELETE ((uint16) 1 << 1)
#define RECOVERY_UPDATE ((uint16) 1 << 2)
#define RECOVERY_COMMIT ((uint16) 1 << 4)
#define RECOVERY_ROLLBACK ((uint16) 1 << 5)
#define RECOVERY_FINISHED ((uint16) 1 << 6)
#define RECOVERY_SYNCHRONIZE ((uint16) 1 << 7)
#define RECOVERY_MODIFY_OXID ((uint16) 1 << 8)
#define RECOVERY_MODIFY_OIDS ((uint16) 1 << 9)
#define RECOVERY_TOAST_CONSISTENT ((uint16) 1 << 10)
#define RECOVERY_SAVEPOINT ((uint16) 1 << 11)
#define RECOVERY_ROLLBACK_TO_SAVEPOINT ((uint16) 1 << 12)
#define RECOVERY_WORKER_PARALLEL_INDEX_BUILD ((uint16) 1 << 13)
#define RECOVERY_LEADER_PARALLEL_INDEX_BUILD ((uint16) 1 << 14)
#define RECOVERY_MODIFY (RECOVERY_INSERT | RECOVERY_DELETE | RECOVERY_UPDATE)
#define RECOVERY_QUEUE_BUF_SIZE (8 * 1024)


typedef struct
{
	uint32		type;
} RecoveryMsgHeader;

typedef struct
{
	RecoveryMsgHeader header;
	OXid		oxid;
	XLogRecPtr	ptr;
} RecoveryMsgOXidPtr;

typedef struct
{
	RecoveryMsgHeader header;
	XLogRecPtr	ptr;
} RecoveryMsgPtr;

typedef struct
{
	RecoveryMsgHeader header;
	Size		o_table_size;
	char		o_table_serialized[FLEXIBLE_ARRAY_MEMBER];
}			RecoveryMsgIdxBuild;

typedef struct
{
	RecoveryMsgHeader header;
	ORelOids	oids;
	OIndexNumber ix_num;
	uint32		o_table_version;
	uint32		current_position;
} RecoveryOidsMsgIdxBuild;

typedef struct
{
	RecoveryMsgHeader header;
} RecoveryMsgEmpty;

typedef struct
{
	uint32		finishRequestCheckpointNumber;
	uint32		immediateRequestCheckpointNumber;
	uint32		completedCheckpointNumber;
	uint32		recoveryMainCompletedCheckpointNumber;
	slock_t		exitLock;
} RecoveryUndoLocFlush;

typedef struct
{
	pg_atomic_uint64 commitPtr;
	pg_atomic_uint64 retainPtr;
	uint32		flushedUndoLocCompletedCheckpointNumber;
} RecoveryWorkerPtrs;

typedef struct
{
	RecoveryMsgHeader header;
	OXid		oxid;
	SubTransactionId parentSubId;
} RecoveryMsgSavepoint;

typedef struct
{
	RecoveryMsgHeader header;
	OXid		oxid;
	XLogRecPtr	ptr;
	SubTransactionId parentSubId;
} RecoveryMsgRollbackToSavepoint;

extern bool toast_consistent;
extern pg_atomic_uint32 *worker_finish_count;
extern pg_atomic_uint32 *idx_worker_finish_count;
extern pg_atomic_uint32 *worker_ptrs_changes;
extern RecoveryWorkerPtrs *worker_ptrs;
extern pg_atomic_uint64 *recovery_ptr;
extern pg_atomic_uint64 *recovery_main_retain_ptr;
extern bool *recovery_single_process;
extern RecoveryUndoLocFlush *recovery_undo_loc_flush;

extern bool *was_in_recovery;
extern pg_atomic_uint32 *after_recovery_cleaned;

/*
 * Recovery master/workers functions.
 */
extern BackgroundWorkerHandle *recovery_worker_register(int worker_id);
PGDLLEXPORT void recovery_worker_main(Datum main_arg);

/*
 * Recovery utility.
 */
extern void apply_modify_record(OTableDescr *descr, OIndexDescr *id,
								uint16 type, OTuple p);
extern bool apply_btree_modify_record(BTreeDescr *tree, uint16 type,
									  OTuple ptr, OXid oxid, CommitSeqNo csn);

extern OBTreeModifyCallbackAction recovery_insert_primary_callback(BTreeDescr *descr,
																   OTuple tup, OTuple *newtup,
																   OXid oxid, OTupleXactInfo xactInfo,
																   UndoLocation location, RowLockMode *lock_mode,
																   BTreeLocationHint *hint,
																   void *arg);
extern OBTreeModifyCallbackAction recovery_delete_primary_callback(BTreeDescr *descr,
																   OTuple tup, OTuple *newtup,
																   OXid oxid, OTupleXactInfo xactInfo,
																   UndoLocation location, RowLockMode *lock_mode,
																   BTreeLocationHint *hint,
																   void *arg);
extern OBTreeModifyCallbackAction recovery_insert_overwrite_callback(BTreeDescr *descr,
																	 OTuple tup, OTuple *newtup,
																	 OXid oxid, OTupleXactInfo xactInfo,
																	 UndoLocation location, RowLockMode *lock_mode,
																	 BTreeLocationHint *hint,
																	 void *arg);
extern OBTreeModifyCallbackAction recovery_delete_overwrite_callback(BTreeDescr *descr,
																	 OTuple tup, OTuple *newtup,
																	 OXid oxid, OTupleXactInfo xactInfo,
																	 UndoLocation location, RowLockMode *lock_mode,
																	 BTreeLocationHint *hint,
																	 void *arg);

extern OBTreeModifyCallbackAction recovery_insert_deleted_primary_callback(BTreeDescr *descr,
																		   OTuple tup, OTuple *newtup,
																		   OXid oxid, OTupleXactInfo xactInfo,
																		   BTreeLeafTupleDeletedStatus deleted,
																		   UndoLocation location, RowLockMode *lock_mode,
																		   BTreeLocationHint *hint,
																		   void *arg);
extern OBTreeModifyCallbackAction recovery_delete_deleted_primary_callback(BTreeDescr *descr,
																		   OTuple tup, OTuple *newtup,
																		   OXid oxid, OTupleXactInfo xactInfo,
																		   BTreeLeafTupleDeletedStatus deleted,
																		   UndoLocation location, RowLockMode *lock_mode,
																		   BTreeLocationHint *hint,
																		   void *arg);
extern OBTreeModifyCallbackAction recovery_insert_deleted_overwrite_callback(BTreeDescr *descr,
																			 OTuple tup, OTuple *newtup,
																			 OXid oxid, OTupleXactInfo xactInfo,
																			 BTreeLeafTupleDeletedStatus deleted,
																			 UndoLocation location, RowLockMode *lock_mode,
																			 BTreeLocationHint *hint,
																			 void *arg);
extern OBTreeModifyCallbackAction recovery_delete_deleted_overwrite_callback(BTreeDescr *descr,
																			 OTuple tup, OTuple *newtup,
																			 OXid oxid, OTupleXactInfo xactInfo,
																			 BTreeLeafTupleDeletedStatus deleted,
																			 UndoLocation location, RowLockMode *lock_mode,
																			 BTreeLocationHint *hint,
																			 void *arg);

#endif							/* __RECOVERY_INTERNAL_H__ */
