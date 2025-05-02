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


extern void register_rewind_worker(void);
PGDLLEXPORT void rewind_worker_main(Datum);
extern Size rewind_shmem_needs(void);
extern void rewind_init_shmem(Pointer buf, bool found);

extern void add_to_rewind_buffer(UndoLogType undoType, OXid oxid, UndoLocation location, bool changeCountsValid);

typedef struct
{
	OXid				oxid;
	UndoLogType			undoType;
	uint64				undoStackLocation;
	TimestampTz			timestamp;
	bool				changeCountsValid;
} RewindItem;

typedef struct
{
	uint64		addPos;			    /* Added to circular buffer */
	uint64		completePos;		/* Removed from circular buffer */
	uint64		evictPos; 			/* Evict/restore position. Evict - left, restore - right */
	uint64		writePos;			/* Written to disk buffers. Increments by bufferLength only */
	uint64		readPos;			/* Read from disk buffer. Increments by bufferLength only */
	uint64		oldCleanedFileNum; 	/* Last removed buffer file number */
	uint64 		freeSpace;			/* Free space in a circular buffer */
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
 * 							    E
 *                  		    A
 *                  		    C
 * |----------|------....|......----|---------|
 * Stage 2
 *                   E
 *                   A          C
 * |----------|------    |      ----|---------|

 * 3. Add more entries to ring buffer
 * 					 E	   A    C
 * |----------|------====|=     ----|---------|
 *
 * 4. Remove entries
 * 			         E     A 	      C
 * |----------|------====|=         | --------|
 *
 * 5. Restore extent from disk:

 * Stage 1 (shift last page part by one page right)
			         E                A
					 				  C
 * |----------|------    |      ====|=--------|
 * Stage 2 (restore oldest disk page before the last circular buffer page)
 * 						 	    E     A
 * 						 	          C
 * |----------|------++++|++++++====|=--------|

 * 6. Remove entries
 * 							    E	  A C
 * |----------|------++++|++++++====|=  ------|
 *
 * 7. Add more entries to ring buffer
 * 										A
 * 							    E		C
 * |----------|------++++|++++++====|===------|
 *
 * 8. Evict extent to disk:
 * Stage 1
 * 										A
 * 				     E 	     	        C
 * |----------|------....|......====|===------|
 * Stage 2 (shift last page part by one page left)
 * 				     E 	     A 	        C
 * |----------|------====|===       |   ------|
 */
 

#endif							/* __REWIND_WORKER_H__ */
