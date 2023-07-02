/*-------------------------------------------------------------------------
 *
 * worker.h
 *		Declarations for S3 worker process.
 *
 * Copyright (c) 2023, OrioleDATA Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/s3/worker.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __S3_WORKER_H__
#define __S3_WORKER_H__

#include "s3/queue.h"

typedef enum
{
	S3TaskTypeWriteFile,
	S3TaskTypeWriteFilePart
} S3TaskType;

/*
 * The data structure representing the task for S3 worker.
 */
typedef struct
{
	S3TaskType	type;
	union
	{
		struct
		{
			uint32		chkpNum;
			char		filename[FLEXIBLE_ARRAY_MEMBER];
		}			writeFile;
		struct
		{
			uint32		chkpNum;
			Oid			datoid;
			Oid			relnode;
			int32		segNum;
			int32		partNum;
		}			writeFilePart;
	}			typeSpecific;
} S3Task;

extern Size s3_workers_shmem_needs(void);
extern void s3_workers_init_shmem(Pointer ptr, bool found);
extern void register_s3worker(int num);
PGDLLEXPORT void s3worker_main(Datum);
extern S3TaskLocation s3_schedule_file_write(uint32 chkpNum, char *filename);
extern S3TaskLocation s3_schedule_file_part_write(uint32 chkpNum, Oid datoid,
												  Oid relnode, int32 segNum,
												  int32 partNum);

#endif							/* __S3_WORKER_H__ */
