/*-------------------------------------------------------------------------
 *
 * control.c
 *		Routines to work with control file.
 *
 * Copyright (c) 2024-2026, Oriole DB Inc.
 * Copyright (c) 2025-2026, Supabase Inc.
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
 * The v1 -> v2 conversion in check_checkpoint_control() reads a version-1
 * control file's CRC from the byte offset that pgVersion now occupies: v1 had
 * no pgVersion field, so its trailing crc sat immediately after s3Mode, exactly
 * where pgVersion was inserted in v2.  Enforce that invariant statically so a
 * future reorder of CheckpointControl (moving pgVersion away from just-before
 * crc, or changing its width) can't silently break reading pre-v2 files -- the
 * offending change trips these asserts and must revisit the conversion instead.
 * New fields must be appended right before crc (bumping the control version and
 * adding a conversion), never inserted earlier.
 */
StaticAssertDecl(offsetof(CheckpointControl, crc) ==
				 offsetof(CheckpointControl, pgVersion) +
				 sizeof(((CheckpointControl *) 0)->pgVersion),
				 "pgVersion must sit immediately before crc so a v1 file's crc aligns with pgVersion");
StaticAssertDecl(sizeof(((CheckpointControl *) 0)->pgVersion) == sizeof(pg_crc32c),
				 "pgVersion must be the same width as the v1 crc it overlays");

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

	/*
	 * Version 1 had no pgVersion field, so its CRC covered fewer bytes and
	 * the on-disk CRC sits where pgVersion now is.  Convert it on the fly:
	 * validate the v1 CRC over the v1 layout and synthesize pgVersion = 0
	 * ("unknown"), which forces the version-dependent sys caches to be
	 * rebuilt -- always safe.  This keeps clusters created before this field
	 * was added readable.
	 */
	if (control->controlFileVersion == 1)
	{
		pg_crc32c	v1crc = *((pg_crc32c *) ((Pointer) control +
											 offsetof(CheckpointControl, pgVersion)));

		INIT_CRC32C(crc);
		COMP_CRC32C(crc, control, offsetof(CheckpointControl, pgVersion));
		FIN_CRC32C(crc);
		if (crc != v1crc)
			elog(ERROR, "Wrong CRC in control file");

		control->pgVersion = 0;
		control->crc = v1crc;
	}
	else
	{
		INIT_CRC32C(crc);
		COMP_CRC32C(crc, control, offsetof(CheckpointControl, crc));
		FIN_CRC32C(crc);

		if (crc != control->crc)
			elog(ERROR, "Wrong CRC in control file");

		if (control->controlFileVersion != ORIOLEDB_CHECKPOINT_CONTROL_VERSION)
			ereport(FATAL,
					(errmsg("checkpoint files are incompatible with server"),
					 errdetail("OrioleDB checkpount control file was initialized with version %d,"
							   " but the currently required version is %d.",
							   control->controlFileVersion, ORIOLEDB_CHECKPOINT_CONTROL_VERSION)));
	}

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

	/* Stamp the writing server's PG version (see CheckpointControl). */
	control->pgVersion = PG_VERSION_NUM;

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
