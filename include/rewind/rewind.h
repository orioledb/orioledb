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

#include "utils/o_buffers.h"

#define REWIND_FILE_SIZE (0x1000000)
#define REWIND_BUFFERS_TAG (0)

OBuffersDesc rewindBuffersDesc;

extern void register_rewind_worker(void);
PGDLLEXPORT void rewind_worker_main(Datum);
extern Size rewind_shmem_needs(void);
extern void rewind_init_shmem(Pointer buf, bool found);

typedef struct
{
	OXid				oxid;
	CommitSeqNo			csn;
	UndoStackLocations	undoStackLocations;
	TimestampTz			timestamp;
} RewindItem;

RewindItem *rewindBuffer;

typedef struct
{
	uint64		addedPos;				/* Added to circular buffer */
	uint64		completedPos;			/* Removed from circular buffer */
	uint64		writtenPos;			/* Written to disk buffers */
	uint64		readPos;			/* Read from disk buffer */
	uint64		oldCleanedFileNum; 	/* Last removed buffer file number */
} RewindMeta;

RewindMeta    *rewindMeta;

#endif							/* __REWIND_WORKER_H__ */
