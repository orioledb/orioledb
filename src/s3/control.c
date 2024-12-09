/*-------------------------------------------------------------------------
 *
 * control.c
 *		Functions to work with S3 control and lock files.
 *
 * Copyright (c) 2024, Oriole DB Inc.
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

#include "miscadmin.h"
#include "storage/fd.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/wait_event.h"

#define LOCK_FILENAME		ORIOLEDB_DATA_DIR "/s3_lock"

/*
 * Read local CheckpointControl file and the file from S3 and check if the
 * S3 bucket compatible with the local instance.
 */
bool
s3_check_control(const char **errmsgp, const char **errdetailp)
{
	CheckpointControl control,
			   *s3_control;
	bool		control_res;
	StringInfoData buf;
	char	   *objectname;
	bool		res = false;

	control_res = get_checkpoint_control_data(&control);

	objectname = psprintf("data/%s", CONTROL_FILENAME);
	initStringInfo(&buf);

	/*
	 * If orioledb_data/control file doesn't exist on the S3 bucket we assume
	 * that it is empty and it is safe to use it without further checks.
	 */
	if (s3_get_object(objectname, &buf, true) == S3_RESPONSE_NOT_FOUND)
	{
		res = true;
		goto cleanup;
	}

	/*
	 * If there is no orioledb_data/control file locally (in case if
	 * CHECKPOINT didn't happen yet) but it exists on the S3 bucket then the
	 * local instance isn't consistent with the S3 bucket.
	 */
	if (!control_res)
	{
		*errmsgp = psprintf("OrioleDB can be incompatible with the S3 bucket "
							"because the control file exists on the S3 bucket");
		*errdetailp = psprintf("OrioleDB control file \"%s\" is absent", CONTROL_FILENAME);
		goto cleanup;
	}

	s3_control = (CheckpointControl *) buf.data;
	check_checkpoint_control(s3_control);

	if (control.controlIdentifier != s3_control->controlIdentifier)
	{
		*errmsgp = psprintf("OrioleDB and the S3 bucket have files from "
							"different instances and they are incompatible with "
							"each other");
		/* cppcheck-suppress unknownMacro */
		*errdetailp = psprintf("OrioleDB control identifier " UINT64_FORMAT
							   " differs from the S3 bucket identifier " UINT64_FORMAT,
							   control.controlIdentifier,
							   s3_control->controlIdentifier);
		goto cleanup;
	}

	if (control.lastCheckpointNumber < s3_control->lastCheckpointNumber)
	{
		*errmsgp = psprintf("OrioleDB misses new changes and checkpoints from "
							"the S3 bucket and they are incompatible with "
							"each other");
		*errdetailp = psprintf("OrioleDB last checkpoint number %u is behind "
							   "the S3 bucket last checkpoint number %u",
							   control.lastCheckpointNumber,
							   s3_control->lastCheckpointNumber);
		goto cleanup;
	}
	else if (control.lastCheckpointNumber > s3_control->lastCheckpointNumber)
		ereport(LOG,
				(errmsg("OrioleDB has more changes and checkpoints than "
						"the S3 bucket but they are still compatible with each "
						"other"),
				 errdetail("OrioleDB last checkpoint number %u is ahead of "
						   "the S3 bucket last checkpoint number %u",
						   control.lastCheckpointNumber,
						   s3_control->lastCheckpointNumber)));

	if (control.sysTreesStartPtr < s3_control->sysTreesStartPtr)
	{
		*errmsgp = psprintf("OrioleDB misses new changes from the S3 bucket "
							"and they are incompatible with each other");
		*errdetailp = psprintf("OrioleDB XLOG location " UINT64_FORMAT
							   " is behind the S3 bucket XLOG location " UINT64_FORMAT,
							   control.sysTreesStartPtr,
							   s3_control->sysTreesStartPtr);
		goto cleanup;
	}
	else if (control.sysTreesStartPtr > s3_control->sysTreesStartPtr)
		ereport(LOG,
				(errmsg("OrioleDB has more changes than the S3 bucket but "
						"they are still compatible with each other"),
				 errdetail("OrioleDB XLOG location " UINT64_FORMAT " is ahead of "
						   "the S3 bucket XLOG location " UINT64_FORMAT,
						   control.sysTreesStartPtr, s3_control->sysTreesStartPtr)));

	res = true;

cleanup:
	pfree(buf.data);
	pfree(objectname);

	return res;
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
	long		res;
	int			retry_put_count = 0;

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

retry_put:
	res = s3_put_file(objectname, LOCK_FILENAME, true);
	if (res == S3_RESPONSE_CONDITION_CONFLICT)
	{
		if (retry_put_count < 10)
		{
			retry_put_count++;

			ereport(LOG,
					(errmsg("the lock file \"%s\" was deleted concurrently, "
							"retrying creating a lock file", objectname)));

			goto retry_put;
		}
		else
			ereport(FATAL,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("failed to create a lock file \"%s\" "
							"because of a concurrent process",
							objectname)));
	}
	else if (res == S3_RESPONSE_CONDITION_FAILED)
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
		else
			ereport(LOG,
					(errmsg("A lock file with the same identifier " UINT64_FORMAT
							" already exists on the S3 bucket",
							lock_identifier)));

		pfree(buf.data);
	}
	else if (res != S3_RESPONSE_OK)
		ereport(FATAL,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("could not put a lock file to S3: %ld", res)));

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
