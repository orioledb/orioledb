/*-------------------------------------------------------------------------
 *
 * control.c
 *		Functions to work with S3 control and lock files.
 *
 * Copyright (c) 2024, OrioleDATA Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/s3/control.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <sys/fcntl.h>
#include <sys/time.h>
#include <unistd.h>

#include "orioledb.h"

#include "checkpoint/control.h"
#include "s3/control.h"
#include "s3/requests.h"

#include "storage/fd.h"
#include "utils/wait_event.h"

#define S3_LOCK_FILENAME    ORIOLEDB_DATA_DIR"/s3_lock"

/*
 * Read local CheckpointControl file and the file from S3 and check if the
 * S3 bucket compatible with the local instance.
 *
 * Returns last checkpoint number.
 */
uint32
s3_check_control(void)
{
	CheckpointControl control,
			   *s3_control;
	StringInfoData buf;
	char	   *objectname;

	get_checkpoint_control_data(&control);

	objectname = psprintf("data/%u/%s",
						  control.lastCheckpointNumber,
						  S3_LOCK_FILENAME);

	initStringInfo(&buf);
	s3_get_object(objectname, &buf);

	s3_control = (CheckpointControl *) &buf.data;
	check_checkpoint_control(s3_control);

	if (control.lastCheckpointNumber != s3_control->lastCheckpointNumber)
		ereport(FATAL,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("OrioleDB files on S3 bucket might be incompatible local files"),
				 errdetail("OrioleDB checkpoint %d is different from S3 bucket checkpoint %d",
						   control.lastCheckpointNumber,
						   s3_control->lastCheckpointNumber)));

	pfree(buf.data);
	pfree(objectname);

	return control.lastCheckpointNumber;
}

/*
 * Try to put a lock file into S3 bucket using conditional write.
 */
void
s3_put_lock_file(uint32 chkpNum)
{
	int			lock_file;
	uint64		lock_identifier = 0;
	char	   *objectname;

	lock_file = BasicOpenFile(S3_LOCK_FILENAME, O_RDONLY | PG_BINARY);
	if (lock_file >= 0)
	{
		pgstat_report_wait_start(WAIT_EVENT_CONTROL_FILE_READ);
		if (read(lock_file, &lock_identifier,
				 sizeof(lock_identifier)) != sizeof(lock_identifier))
			ereport(FATAL,
					(errcode_for_file_access(),
					 errmsg("could not read data from lock file \"%s\"",
							S3_LOCK_FILENAME)));
		pgstat_report_wait_end();

		close(lock_file);

		if (lock_identifier == 0)
			ereport(FATAL,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("incorrect value of lock identifier " UINT64_FORMAT,
							lock_identifier)));
	}
	else
	{
		struct timeval tv;

		if (errno != ENOENT)
			ereport(FATAL,
					(errcode_for_file_access(),
					 errmsg("could not open file \"%s\": %m", S3_LOCK_FILENAME)));

		/*
		 * Calculate a lock identifier similar to how PostgreSQL calculates a
		 * system identifier.
		 */
		gettimeofday(&tv, NULL);
		lock_identifier = ((uint64) tv.tv_sec) << 32;
		lock_identifier |= ((uint64) tv.tv_usec) << 12;
		lock_identifier |= getpid() & 0xFFF;

		lock_file = BasicOpenFile(S3_LOCK_FILENAME, O_WRONLY | O_CREAT | PG_BINARY);
		if (lock_file < 0)
			ereport(FATAL,
					(errcode_for_file_access(),
					 errmsg("could not create file \"%s\": %m", S3_LOCK_FILENAME)));

		pgstat_report_wait_start(WAIT_EVENT_CONTROL_FILE_WRITE);
		if (write(lock_file, &lock_identifier,
				  sizeof(lock_identifier)) != sizeof(lock_identifier))
			ereport(FATAL,
					(errcode_for_file_access(),
					 errmsg("could not write file \"%s\": %m",
							S3_LOCK_FILENAME)));
		pgstat_report_wait_end();

		pgstat_report_wait_start(WAIT_EVENT_CONTROL_FILE_SYNC);
		if (pg_fsync(lock_file) != 0)
			ereport(FATAL,
					(errcode_for_file_access(),
					 errmsg("could not fsync file \"%s\": %m",
							S3_LOCK_FILENAME)));
		pgstat_report_wait_end();

		close(lock_file);
	}


	objectname = psprintf("data/%u/%s", chkpNum, S3_LOCK_FILENAME);

	pfree(objectname);
}
