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
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/wait_event.h"

#define LOCK_FILENAME		ORIOLEDB_DATA_DIR "/s3_lock"

/*
 * Read local CheckpointControl file and the file from S3 and check if the
 * S3 bucket compatible with the local instance.
 */
void
s3_check_control(void)
{
	CheckpointControl control,
			   *s3_control;
	StringInfoData buf;
	char	   *objectname;

	if (!get_checkpoint_control_data(&control))
		return;

	objectname = psprintf("data/%s", CONTROL_FILENAME);

	initStringInfo(&buf);
	if (!s3_get_object(objectname, &buf, true))
	{
		pfree(buf.data);
		pfree(objectname);
		return;
	}

	s3_control = (CheckpointControl *) buf.data;
	check_checkpoint_control(s3_control);

	if (control.lastCheckpointNumber != s3_control->lastCheckpointNumber)
		ereport(FATAL,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("OrioleDB files on the S3 bucket might be incompatible with local files"),
				 errdetail("OrioleDB last checkpoint number %u"
						   " is different from the S3 bucket last checkpoint number %u",
						   control.lastCheckpointNumber, s3_control->lastCheckpointNumber)));

	if (control.sysTreesStartPtr < s3_control->sysTreesStartPtr)
		ereport(FATAL,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("OrioleDB files on the S3 bucket might be incompatible with local files"),
				 errdetail("OrioleDB XLOG location " UINT64_FORMAT
						   " is behind from the S3 bucket XLOG location " UINT64_FORMAT,
						   control.sysTreesStartPtr, s3_control->sysTreesStartPtr)));

	pfree(buf.data);
	pfree(objectname);
}

/*
 * Try to put a lock file into S3 bucket using conditional write.
 */
void
s3_put_lock_file(void)
{
	int			lock_file;
	uint64		lock_identifier = 0;
	char	   *objectname;

	lock_file = BasicOpenFile(LOCK_FILENAME, O_RDONLY | PG_BINARY);
	if (lock_file >= 0)
	{
		if (read(lock_file, &lock_identifier,
				 sizeof(lock_identifier)) != sizeof(lock_identifier))
			ereport(FATAL,
					(errcode_for_file_access(),
					 errmsg("could not read data from lock file \"%s\"",
							LOCK_FILENAME)));

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
					 errmsg("could not open file \"%s\": %m", LOCK_FILENAME)));

		/*
		 * Calculate a lock identifier similar to how PostgreSQL calculates a
		 * system identifier.
		 */
		gettimeofday(&tv, NULL);
		lock_identifier = ((uint64) tv.tv_sec) << 32;
		lock_identifier |= ((uint64) tv.tv_usec) << 12;
		lock_identifier |= getpid() & 0xFFF;

		lock_file = BasicOpenFile(LOCK_FILENAME, O_WRONLY | O_CREAT | PG_BINARY);
		if (lock_file < 0)
			ereport(FATAL,
					(errcode_for_file_access(),
					 errmsg("could not create file \"%s\": %m", LOCK_FILENAME)));

		if (write(lock_file, &lock_identifier,
				  sizeof(lock_identifier)) != sizeof(lock_identifier))
			ereport(FATAL,
					(errcode_for_file_access(),
					 errmsg("could not write file \"%s\": %m",
							LOCK_FILENAME)));

		if (pg_fsync(lock_file) != 0)
			ereport(FATAL,
					(errcode_for_file_access(),
					 errmsg("could not fsync file \"%s\": %m",
							LOCK_FILENAME)));

		close(lock_file);
	}

	elog(DEBUG1, "lock_identifier: " UINT64_FORMAT, lock_identifier);

	objectname = psprintf("data/%s", LOCK_FILENAME);
	if (!s3_put_file(objectname, LOCK_FILENAME, true))
	{
		StringInfoData buf;
		uint64		s3_lock_identifier;

		/*
		 * The lock file exists on the S3 bucket. In this case check its lock
		 * identifier. If it is same as the local identifier then proceed with
		 * startup.
		 */
		initStringInfo(&buf);

		s3_get_object(objectname, &buf, false);

		if (buf.len != sizeof(s3_lock_identifier))
			ereport(FATAL,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("Invalid lock identifier \"%s\" in the S3 bucket",
							objectname)));

		memcpy((char *) &s3_lock_identifier, buf.data, sizeof(s3_lock_identifier));

		if (lock_identifier != s3_lock_identifier)
			ereport(FATAL,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("A lock file from a different OrioleDB instance already exists on the S3 bucket"),
					 errdetail("The local lock identifier " UINT64_FORMAT " is "
							   "different from the S3 bucket identifier " UINT64_FORMAT,
							   lock_identifier, s3_lock_identifier)));

		pfree(buf.data);
	}

	pfree(objectname);
}

/*
 * Delete a lock file from an S3 bucket.
 */
void
s3_delete_lock_file(void)
{
	char	   *objectname;

	objectname = psprintf("data/%s", LOCK_FILENAME);

	pgstat_report_wait_start(WAIT_EVENT_CONTROL_FILE_WRITE_UPDATE);
	s3_delete_object(objectname);
	pgstat_report_wait_end();

	pfree(objectname);
}
