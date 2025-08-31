/*-------------------------------------------------------------------------
 *
 * control.c
 *		Routines to work with control file.
 *
 * Copyright (c) 2024-2025, Oriole DB Inc.
 * Copyright (c) 2025, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/checkpoint/control.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <unistd.h>

#include "orioledb.h"

#include "btree/io.h"
#include "checkpoint/control.h"

#include "utils/wait_event.h"

/*
 * Read checkpoint control file data from the disk.
 *
 * Returns false if the control file doesn't exist.
 */
bool
get_checkpoint_control_data(CheckpointControl *control)
{
	int			controlFile;
	Size		readBytes;

	controlFile = BasicOpenFile(CONTROL_FILENAME, O_RDONLY | PG_BINARY);
	if (controlFile < 0)
	{
		/*
		 * If we couldn't find the control file the we consider this case as
		 * if there wasn't any checkpoint before.
		 */
		if (errno == ENOENT)
			return false;

		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m",
						CONTROL_FILENAME)));
	}

	readBytes = read(controlFile, (Pointer) control, sizeof(CheckpointControl));

	/*
	 * Handle special case when the control file is empty.  We consider this
	 * case as if there wasn't created the control file and checkpoint never
	 * finished successfully.
	 */
	if (readBytes == 0)
		return false;
	else if (readBytes != sizeof(CheckpointControl))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read data from control file \"%s\": %m",
						CONTROL_FILENAME)));

	close(controlFile);

	check_checkpoint_control(control);

	return true;
}

/*
 * Check checkpoint control data
 *   - Check CRC
 *   - Check control parameters
 */
void
check_checkpoint_control(CheckpointControl *control)
{
	pg_crc32c	crc;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, control, offsetof(CheckpointControl, crc));
	FIN_CRC32C(crc);

	if (crc != control->crc)
		elog(ERROR, "Wrong CRC in control file");

	if (control->binaryVersion != ORIOLEDB_BINARY_VERSION)
		ereport(FATAL,
				(errmsg("database files are incompatible with server"),
				 errdetail("OrioleDB was initialized with binary version %d,"
						   " but the extension is compiled with binary version %d.",
						   control->binaryVersion, ORIOLEDB_BINARY_VERSION),
				 errhint("It looks like you need to initdb.")));

	if (control->s3Mode != orioledb_s3_mode)
		ereport(FATAL,
				(errmsg("database files are incompatible with server"),
				 errdetail("OrioleDB was initialized with S3 mode %s,"
						   " but the extension is configured with S3 mode %s.",
						   control->s3Mode ? "on" : "off",
						   orioledb_s3_mode ? "on" : "off")));
}

/*
 * Write checkpoint control file to the disk (and sync).
 */
void
write_checkpoint_control(CheckpointControl *control)
{
	File		controlFile;
	char		buffer[CHECKPOINT_CONTROL_FILE_SIZE];

	INIT_CRC32C(control->crc);
	COMP_CRC32C(control->crc, control, offsetof(CheckpointControl, crc));
	FIN_CRC32C(control->crc);

	memset(buffer, 0, CHECKPOINT_CONTROL_FILE_SIZE);
	memcpy(buffer, control, sizeof(CheckpointControl));

	controlFile = PathNameOpenFile(CONTROL_FILENAME, O_RDWR | O_CREAT | PG_BINARY);
	if (controlFile < 0)
		ereport(FATAL, (errcode_for_file_access(),
						errmsg("could not open checkpoint control file %s: %m", CONTROL_FILENAME)));

	if (OFileWrite(controlFile, buffer, CHECKPOINT_CONTROL_FILE_SIZE, 0,
				   WAIT_EVENT_SLRU_WRITE) != CHECKPOINT_CONTROL_FILE_SIZE ||
		FileSync(controlFile, WAIT_EVENT_SLRU_SYNC) != 0)
		ereport(FATAL, (errcode_for_file_access(),
						errmsg("could not write checkpoint control to file %s: %m", CONTROL_FILENAME)));

	FileClose(controlFile);
}
