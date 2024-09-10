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

#include <sys/time.h>
#include <unistd.h>

#include "orioledb.h"

#include "checkpoint/control.h"
#include "s3/control.h"
#include "s3/requests.h"

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
						  CONTROL_FILENAME);

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
	uint64		lock_identifier;
	struct timeval tv;
	char	   *objectname;

	/*
	 * Calculate a lock identifier similar to how PostgreSQL calculates a
	 * system identifier.
	 */
	gettimeofday(&tv, NULL);
	lock_identifier = ((uint64) tv.tv_sec) << 32;
	lock_identifier |= ((uint64) tv.tv_usec) << 12;
	lock_identifier |= getpid() & 0xFFF;

	objectname = psprintf("data/%u/%s", chkpNum, S3_LOCK_FILENAME);

	pfree(objectname);
}
