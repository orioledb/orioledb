/*-------------------------------------------------------------------------
 *
 * queue.h
 * 		A declarations for queue of tasks for S3 workers.
 *
 * Copyright (c) 2023, OrioleDATA Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/s3/queue.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __S3_QUEUE_H__
#define __S3_QUEUE_H__

#include "postgres.h"

#include "orioledb.h"

/*
 * Meta-information about S3 tasks queue.
 */
typedef struct
{
	/* Location to insert new tasks */
	pg_atomic_uint64 insertLocation;
	ConditionVariable insertLocationCV;

	/* Location to pick the existing tasks by workers */
	pg_atomic_uint64 pickLocation;

	/*
	 * All the tasks before this location has been already erased in the
	 * buffer.
	 */
	pg_atomic_uint64 erasedLocation;
	ConditionVariable erasedLocationCV;
} S3TaskQueueMeta;

typedef uint64 S3TaskLocation;

#define InvalidS3TaskLocation (UINT64_MAX)

extern Size s3_queue_shmem_needs(void);
extern void s3_queue_init_shmem(Pointer ptr, bool found);
extern S3TaskLocation s3_queue_put_task(Pointer data, uint32 len);
extern S3TaskLocation s3_queue_try_pick_task(void);
Pointer		s3_queue_get_task(S3TaskLocation taskLocation);
extern void s3_queue_erase_task(S3TaskLocation taskLocation);
extern void s3_queue_wait_for_location(S3TaskLocation location);

#endif							/* __S3_QUEUE_H__ */
