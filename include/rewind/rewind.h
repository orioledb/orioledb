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

#define EMPTY_ITEM_TAG (0)
#define REWIND_ITEM_TAG (1)
#define SUBXIDS_ITEM_TAG (2)

#define SUBXIDS_PER_ITEM	(25)

/* RewindItem and SubxidsItem should have same size to be castable to each other */
/* Empty RewindItem and SubxidsItem have invalid oxid and tag */
typedef	struct RewindItem
{
	uint8		tag;
	int 		nsubxids;
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
	uint8           tag;
	int		nsubxids;
	OXid            oxid;	/* Redundant, for debug */
	TransactionId   subxids[SUBXIDS_PER_ITEM];
} SubxidsItem;

#define REWIND_DISK_BUFFER_LENGTH (ORIOLEDB_BLCKSZ / sizeof(RewindItem))

typedef struct
{
	pg_atomic_uint64 addPosReserved; /* Next adding position available for concurrent add process */
	pg_atomic_uint64 addPosFilled;	 /* Position that is already added and it could be evicted */
	uint64		completePos;	/* Next complete position */
	uint64		evictPos;	/* Next evict position. Increments by bufferLength only */
	uint64		restorePos;	/* Next restore after eviction position. Increments by bufferLength only */
	uint64		checkpointPos;	/* Already included into checkpoint. Start
								 * point for writing rewindItem-s into
								 * checkpoint. */
	uint64		oldCleanedFileNum;	/* Last removed buffer file number */
	int16		addInProgress;  /* Number of concurrent items being added at the moment */
	bool		skipCheck;		/* Skip timestamp-based check of items to
								 * process */
	int		rewindEvictTrancheId;
	LWLock		rewindEvictLock;		/* Lock during evict/restore page from circular buffer against concurrent eviction */
	int		rewindCheckpointTrancheId;
	LWLock		rewindCheckpointLock;		/* Lock during checkpointing againts concurrent eviction */
	pg_atomic_uint64 oldestConsideredRunningXid;
	pg_atomic_uint64 runXmin;
	bool 		rewindWorkerStopRequested;
	bool		rewindWorkerStopped;
	bool 		addToRewindQueueDisabled;
	TransactionId	force_complete_xid;
	OXid		force_complete_oxid;

} RewindMeta;

#define InvalidRewindPos UINT64_MAX

#endif							/* __REWIND_WORKER_H__ */
