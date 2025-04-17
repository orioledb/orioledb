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

//extern bool IsBGWriter;

extern void register_rewindworker(void);
PGDLLEXPORT void rewind_worker_main(Datum);

typedef struct
{
	OXid				oxid;
	UndoStackLocations	undoStackLocations;
	TimestampTz			timestamp;
} RewindItem

typedef struct
{
	uint64		addPos;		/* Added to circular buffer */
	uint64		cleanedPos; /* Removed from circular buffer */
	uint64		writtenPos; /* Written to disk buffers */
	uint64		readPos;	/* Read from disk buffer */
} RewindMeta

#endif							/* __REWIND_WORKER_H__ */
