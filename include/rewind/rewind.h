/*-------------------------------------------------------------------------
 *
 * rewind.h
 *		Routines for background writer process.
 *
 * Copyright (c) 2021-2025, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/workers/rewind.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __REWIND_WORKER_H__
#define __REWIND_WORKER_H__

#include "c.h"
#include "utils/o_buffers.h"

#include "access/transam.h"

#define REWIND_FILE_SIZE (0x1000000)
#define REWIND_BUFFERS_TAG (0)

extern TransactionId orioledb_vacuum_horizon_hook(void);
extern void register_rewind_worker(void);
extern bool is_rewind_worker(void);
PGDLLEXPORT void rewind_worker_main(Datum);
extern Size rewind_shmem_needs(void);
extern void rewind_init_shmem(Pointer buf, bool found);
extern void checkpoint_write_rewind_xids(void);
extern void add_to_rewind_buffer(OXid oxid, TransactionId xid, int nsubxids, TransactionId *subxids);
extern void save_precommit_xid_subxids(void);
extern TransactionId get_precommit_xid_subxids(int *nsubxids, TransactionId **subxids);
extern void reset_precommit_xid_subxids(void);
extern OXid get_rewind_run_xmin(void);
extern void log_print_rewind_queue(void);

#define EMPTY_ITEM_TAG (0)
#define REWIND_ITEM_TAG (1)
#define SUBXIDS_ITEM_TAG (2)

#define SUBXIDS_PER_ITEM	(25)

/* RewindItem and SubxidsItem should have same size to be castable to each other */
/* Empty RewindItem and SubxidsItem have invalid oxid and tag */
typedef struct RewindItem
{
	uint8		tag;
	int			nsubxids;
	OXid		oxid;
	TransactionId xid;			/* regular transaction id if any */
	uint64		onCommitUndoLocation[UndoLogsCount];
	uint64		undoLocation[UndoLogsCount];
	uint64		minRetainLocation[UndoLogsCount];
	FullTransactionId oldestConsideredRunningXid;
	OXid		runXmin;
	TimestampTz timestamp;
} RewindItem;

typedef struct SubxidsItem
{
	uint8		tag;
	int			nsubxids;
	OXid		oxid;			/* Redundant, for debug */
	TransactionId subxids[SUBXIDS_PER_ITEM];
} SubxidsItem;

#define REWIND_DISK_BUFFER_LENGTH (ORIOLEDB_BLCKSZ / sizeof(RewindItem))

typedef struct
{
	pg_atomic_uint64 addPosReserved;	/* Next adding position available for
										 * concurrent add process */
	pg_atomic_uint64 addPosFilled;	/* Position that is already added and it
									 * could be evicted */
	uint64		completePos;	/* Next complete position */
	uint64		evictPos;		/* Next evict position. Increments by
								 * bufferLength only */
	uint64		restorePos;		/* Next restore after eviction position.
								 * Increments by bufferLength only */
	uint64		checkpointPos;	/* Already included into checkpoint. Start
								 * point for writing rewindItem-s into
								 * checkpoint. */
	uint64		oldCleanedFileNum;	/* Last removed buffer file number */
	int16		addInProgress;	/* Number of concurrent items being added at
								 * the moment */
	bool		skipCheck;		/* Skip timestamp-based check of items to
								 * process */
	int			rewindEvictTrancheId;
	LWLock		rewindEvictLock;	/* Lock during evict/restore page from
									 * circular buffer against concurrent
									 * eviction */
	int			rewindCheckpointTrancheId;
	LWLock		rewindCheckpointLock;	/* Lock during checkpointing againts
										 * concurrent eviction */
	pg_atomic_uint64 oldestConsideredRunningXid;
	pg_atomic_uint64 runXmin;
	bool		rewindWorkerStopRequested;
	bool		rewindWorkerStopped;
	bool		addToRewindQueueDisabled;
	TransactionId force_complete_xid;
	OXid		force_complete_oxid;

} RewindMeta;

/*
 * Queue of rewind items internals:
 * --------------------------------
 *                                                                                               F R
 *  |      C+++++++R++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++E++++++++++++A.A       |
 *  C - completePos, R-restorePos, E-evictPos, AF - addPosFilled, AR - addPosReserved
 *  " " - empty items, "+" - busy items, "." - transient items
 *  All positions are uint64 and can only increase.
 *  E and R are modified only under lock.
 *  C can only be increased by the only rewind worker and is not locked.
 *  A(F) and A(R) - are atomic uint64 as they are accessed concurrently by adding backends at commit transaction.
 *
 *  All readable items are from C to AF. Between AF and AR some items are transiently not filled, so they
 *   are not readable by evict/restore/complete/rewind. But up to A(R) position they are is considered "busy"
 *   and A(R) is used for calculation of free space for eviction.
 *
 * Tail and head of this queue are placed into in-memory buffers completeBuffer and addBuffer.
 * They use modulo placement so every time ( R-C < buffer_length ) and ( AR - E < buffer_length).
 *
 *     |      C+++++R      |        or alternatively         |++++R      C+++++|
 *
 *                  F R                                            F R
 *       |      E+++A.A      |      or alternatively           |+++A.A      E+++++|
 *
 * Positions between R and E are written to disk (in full blocks only).
 *
 * There are following actions on a queue:
 *  - Adding: to addBuffer, done by committing backends
 *  - Eviction: from addBuffer, done by committing backends
 *  - Restore: to completeBuffer, done by a rewind worker
 *  - Rewind: from all buffers, done by a backend where rewind is requested. All other backends are shut down
 *  before actual Rewind starts.
 *  - Complete: from completeBuffer, done by a rewind worker
 *
 *  Adding to addBuffer:
 *  (1) checks is the space enough, if not calls eviction first
 *  (2) reserves space to addind by incrementing AR
 *  (3) adds items to the reserved space:
 *
 * Rewind queue items has two types
 *  - rewindItem: mandatory for each transaction commit
 *  - subxidsItem: optional, when subxids exist for a transaction.
 *
 * All items for the same transaction are written together in the following order:
 *  rewindItem->subxidsItem(1)->...->subxidsItem(n). This is crucial for proper backwards reading during Rewind.
 *
 *  (4) pushes AF:
 *   - if all concurrent backends that incremented AR finished
 *   - if transient difference AR-AF becomes big (then transaction waits concurrent ones to complete)
 *
 * Eviction from addBuffer has two ways:
 * - fast: direct transfer from addBuffer to completeBuffer (in there is space for this)
 * - ordinary: items written to disk and cleared from addBuffer (by full blocks)
 *
 * Restore to completeBuffer also has two ways:
 * - ordinary: if some items are evicted to disk but not restored yet. So they are read from disk to free
 *  space in conpleteBuffer (by full blocks);
 * - fast: - if items are only in in-memory buffers. So they are directly transferred from addBuffer to
 *  completeBuffer.
 *
 * At Rewind, backend reads all existing items from both in-memory buffers and on-disk buffer in reverse
 * so from AF to C. Only one backend remains at the time of do_rewind() starts so it works only serially and doesn't
 * need locks. RewindWorker applies undoChain from each item and restores global variables relevant to heap
 * transaction visibility. Rewind stops (1) on reaching specified xid/oxid pair or time values (2) on reaching
 * completePos, so maximum depth of rewind is limited by a rewind buffer.
 *
 * At Complete, rewind worker fix both Oriole and heap transactions remembered in queue rewindItems from C to AF.
 * This is done serially but may require restore action when completeBuffer frees enough. Restore takes evictLock
 * to avoid concurrent eviction by backends.
 *
 * Eviction has two locks:
 * 	rewindEvictLock - against concurrent eviction (1) and eviction during restore (2). Only backend that got
 * 	eviction lock does eviction, others skip and do not wait.
 *
 * 	rewindCheckpointLock - against eviction during checkpoint. All backends that couldn't get it wait until
 * 	checkpoint releases the lock.
 *
 *
 */

#define InvalidRewindPos UINT64_MAX

#endif							/* __REWIND_WORKER_H__ */
