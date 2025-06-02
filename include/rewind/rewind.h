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

#define EMPTY_ITEM_TAG (0)
#define REWIND_ITEM_TAG (1)
#define SUBXIDS_ITEM_TAG (2)

#define SUBXIDS_PER_ITEM	(23)

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
	pg_atomic_uint64 addPos;	/* Added to circular buffer */
	uint64		completePos;	/* Removed from circular buffer */
	uint64		evictPos;		/* Evict/restore position. Evict - left,
								 * restore - right */
	uint64		writePos;		/* Written to disk buffers. Increments by
								 * bufferLength only */
	uint64		readPos;		/* Read from disk buffer. Increments by
								 * bufferLength only */
	uint64		checkpointPos;	/* Already included into checkpoint. Start
								 * point for writing rewindItem-s into
								 * checkpoint. */
	uint64		oldCleanedFileNum;	/* Last removed buffer file number */
	bool		skipCheck;		/* Skip timestamp-based check of items to
								 * process */
	int		rewindEvictTrancheId;
	LWLock		evictLock;		/* Lock to evict page from circular buffer */
	pg_atomic_uint64 oldestConsideredRunningXid;
	bool 		rewindWorkerStopRequested;
	bool		rewindWorkerStopped;
	bool 		addToRewindQueueDisabled;

} RewindMeta;

#define InvalidRewindPos UINT64_MAX

/* Circular buffer:
 * A - addedPos, C - completedPos, E - evictPos
 * freeSpace - A -> C
 * -> Direction of adding new/removing completed.
 *
 * 1. Only ring buffer E = 0
 * 0                      A     C
 * |----------|----------|-     ----|---------|
 *
 * 2. Evict extent to disk
 * Stage 1 (before inserting item that doesn't fit buffer)
 *                              E
 *                              A
 *                              C
 * |----------|------....|......----|---------|
 * Stage 2
 *                   E
 *                   A          C
 * |----------|------    |      ----|---------|
 *
 * 3. Add more entries to ring buffer
 *                   E     A    C
 * |----------|------====|=     ----|---------|
 *
 * 4. Remove entries
 *                   E     A 	      C
 * |----------|------====|=         | --------|
 *
 * 5. Restore extent from disk:
 *
 * Stage 1 (shift last page part by one page right)
 *                   E                A
 *                                    C
 * |----------|------    |      ====|=--------|
 * Stage 2 (restore oldest disk page before the last circular buffer page)
 *                              E     A
 *                                    C
 * |----------|------++++|++++++====|=--------|
 *
 * 6. Remove entries
 *                              E     A C
 * |----------|------++++|++++++====|=  ------|
 *
 * 7. Add more entries to ring buffer
 *                                      A
 *                              E       C
 * |----------|------++++|++++++====|===------|
 *
 * 8. Evict extent to disk:
 * Stage 1
 *                                      A
 *                   E                  C
 * |----------|------....|......====|===------|
 * Stage 2 (shift last page part by one page left)
 *                   E 	     A 	        C
 * |----------|------====|===       |   ------|
 *
 */


#endif							/* __REWIND_WORKER_H__ */
