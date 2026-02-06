/*-------------------------------------------------------------------------
 *
 * control.h
 *		Declarations for control file.
 *
 * Copyright (c) 2024-2026, Oriole DB Inc.
 * Copyright (c) 2025-2026, Supabase Inc.
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
	UndoLocation lastUndoLocation;
	UndoLocation checkpointRetainStartLocation;
	UndoLocation checkpointRetainEndLocation;
} CheckpointUndoInfo;

#define NUM_CHECKPOINTABLE_UNDO_LOGS	2

/*
 * Bump every time CheckpointControl structure changes its format.
 * Also on-the-flight conversion routine should be added to
 * check_checkpoint_control()
 */
#define ORIOLEDB_CHECKPOINT_CONTROL_VERSION	1

/*
 * To ensure correct reading of controlFileVersion, changes in struct layout
 * are permitted only between .controlFileVersion and .crc, that
 * should be the last.
 */
typedef struct
{
	uint64		controlIdentifier;
	uint32		lastCheckpointNumber;
	uint32		controlFileVersion;
	CommitSeqNo lastCSN;
	OXid		lastXid;
	UndoLocation lastUndoLocation;
	XLogRecPtr	toastConsistentPtr;
	XLogRecPtr	replayStartPtr;
	XLogRecPtr	sysTreesStartPtr;
	uint64		mmapDataLength;
	CheckpointUndoInfo undoInfo[NUM_CHECKPOINTABLE_UNDO_LOGS];
	UndoLocation checkpointRetainStartLocation;
	UndoLocation checkpointRetainEndLocation;
	OXid		checkpointRetainXmin;
	OXid		checkpointRetainXmax;
	uint32		binaryVersion;
	bool		s3Mode;
	/* CRC of all fields above. It should be last */
	pg_crc32c	crc;
} CheckpointControl;

#define CONTROL_FILENAME    ORIOLEDB_DATA_DIR"/control"

#define GetCheckpointableUndoLog(i) \
	(AssertMacro((i) >= 0 && (i) < 2), \
		(i) == 0 ? UndoLogRegular : UndoLogSystem)

/*
 * Physical size of the orioledb_data/control file.  Note that this is considerably
 * bigger than the actually used size (ie, sizeof(CheckpointControl)).
 * The idea is to keep the physical size constant independent of format
 * changes, so that get_checkpoint_control_data will deliver a suitable wrong-version
 * message instead of a read error if it's looking at an incompatible file.
 */
#define CHECKPOINT_CONTROL_FILE_SIZE	8192

extern bool get_checkpoint_control_data(CheckpointControl *control);
extern void check_checkpoint_control(CheckpointControl *control);
extern void write_checkpoint_control(CheckpointControl *control);

#endif							/* __CHECKPOINT_CONTROL_H__ */
