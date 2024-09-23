/*-------------------------------------------------------------------------
 *
 * control.h
 *		Declarations for control file.
 *
 * Copyright (c) 2024, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/checkpoint/control.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __CHECKPOINT_CONTROL_H__
#define __CHECKPOINT_CONTROL_H__

#include "postgres.h"

#include "orioledb.h"

typedef struct
{
	uint64		control_identifier;
	uint32		lastCheckpointNumber;
	CommitSeqNo lastCSN;
	OXid		lastXid;
	UndoLocation lastUndoLocation;
	XLogRecPtr	toastConsistentPtr;
	XLogRecPtr	replayStartPtr;
	XLogRecPtr	sysTreesStartPtr;
	uint64		mmapDataLength;
	UndoLocation checkpointRetainStartLocation;
	UndoLocation checkpointRetainEndLocation;
	OXid		checkpointRetainXmin;
	OXid		checkpointRetainXmax;
	uint32		binaryVersion;
	bool		s3Mode;
	pg_crc32c	crc;
} CheckpointControl;

#define CONTROL_FILENAME    ORIOLEDB_DATA_DIR"/control"

extern bool get_checkpoint_control_data(CheckpointControl *control);
extern void check_checkpoint_control(CheckpointControl *control);
extern void write_checkpoint_control(CheckpointControl *control);

#endif							/* __CHECKPOINT_CONTROL_H__ */
