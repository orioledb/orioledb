/*-------------------------------------------------------------------------
 *
 * queue.c
 *		Implementation for queue of tasks for S3 workers.
 *
 * Copyright (c) 2023, OrioleDATA Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/s3/queue.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "orioledb.h"

#include "s3/queue.h"

#include "utils/wait_event.h"

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

/*
 * The "erased" flag for the task length.  This flag means that task body was
 * already erased, but erased position wasn't yet advanced through this task.
 */
#define	LENGTH_ERASED_FLAG	(0x80000000)

static Size s3_queue_size = 0;
static S3TaskQueueMeta *s3_queue_meta = NULL;
static Pointer s3_queue_buffer = NULL;

Size
s3_queue_shmem_needs(void)
{
	Size		size = 0;

	if (!orioledb_s3_mode)
		return size;

	size = add_size(size, CACHELINEALIGN(sizeof(S3TaskQueueMeta)));
	size = add_size(size, CACHELINEALIGN((Size) s3_queue_size_guc * 1024));

	return size;
}

void
s3_queue_init_shmem(Pointer ptr, bool found)
{
	if (!orioledb_s3_mode)
		return;

	s3_queue_size = (Size) s3_queue_size_guc * 1024;

	s3_queue_meta = (S3TaskQueueMeta *) ptr;
	ptr += CACHELINEALIGN(sizeof(S3TaskQueueMeta));

	s3_queue_buffer = ptr;

	if (!found)
	{
		pg_atomic_init_u64(&s3_queue_meta->insertLocation, 0);
		pg_atomic_init_u64(&s3_queue_meta->pickLocation, 0);
		pg_atomic_init_u64(&s3_queue_meta->erasedLocation, 0);

		ConditionVariableInit(&s3_queue_meta->insertLocationCV);
		ConditionVariableInit(&s3_queue_meta->erasedLocationCV);

		memset(s3_queue_buffer, 0, s3_queue_size);
	}
}

S3TaskLocation
s3_queue_get_insert_location(void)
{
	return pg_atomic_read_u64(&s3_queue_meta->insertLocation);
}

/*
 * Put new task to the lockless queue.
 */
S3TaskLocation
s3_queue_put_task(Pointer data, uint32 len)
{
	S3TaskLocation insertLocation;
	bool		slept = false;
	uint32		totallen = len + sizeof(uint32);

	Assert(totallen = INTALIGN(totallen));

	/* Pick the insert location */
	insertLocation = pg_atomic_fetch_add_u64(&s3_queue_meta->insertLocation, totallen);

	/*
	 * Check that circular buffer of tasks didn't wraparound.  Wait the
	 * overlapping tasks to be erased before we continue.
	 */
	while (insertLocation + totallen > pg_atomic_read_u64(&s3_queue_meta->erasedLocation) + s3_queue_size)
	{
		ConditionVariableSleep(&s3_queue_meta->erasedLocationCV, WAIT_EVENT_MQ_PUT_MESSAGE);
		slept = true;
	}
	if (slept)
		ConditionVariableCancelSleep();

	/* Put the task into a circular buffer */
	if (insertLocation / s3_queue_size == (insertLocation + totallen - 1) / s3_queue_size)
	{
		/* Easy case: we can put the task a as continuous chunk of memory */
		memcpy(s3_queue_buffer + insertLocation % s3_queue_size + sizeof(uint32),
			   data,
			   len);
	}
	else
	{
		/*
		 * More complex case: we hit the buffer end boundary.  In this case we
		 * need to split the task into two distinct chunks.
		 */
		uint32		firstChunkLen = s3_queue_size - insertLocation % s3_queue_size;

		Assert(firstChunkLen >= sizeof(uint32));

		memcpy(s3_queue_buffer + insertLocation % s3_queue_size + sizeof(uint32),
			   data,
			   firstChunkLen - sizeof(uint32));
		memcpy(s3_queue_buffer,
			   data + (firstChunkLen - sizeof(uint32)),
			   totallen - firstChunkLen);
	}

	/*
	 * Write the task length after copying the task body.  We use length
	 * presence as the sign that body is completely copied.
	 */
	pg_write_barrier();
	*((uint32 *) (s3_queue_buffer + insertLocation % s3_queue_size)) = totallen;

	return insertLocation;
}

/*
 * Try to pick the task for processing.  Returns the task location on success,
 * and InvalidS3TaskLocation on failure.
 */
S3TaskLocation
s3_queue_try_pick_task(void)
{
	while (true)
	{
		S3TaskLocation insertLocation,
					pickLocation,
					erasedLocation;
		uint32		taskLen;

		pickLocation = pg_atomic_read_u64(&s3_queue_meta->pickLocation);
		pg_read_barrier();
		insertLocation = pg_atomic_read_u64(&s3_queue_meta->insertLocation);
		erasedLocation = pg_atomic_read_u64(&s3_queue_meta->erasedLocation);

		if (pickLocation >= insertLocation)
		{
			/* Nothing inserted yet */
			Assert(pickLocation == insertLocation);
			return InvalidS3TaskLocation;
		}

		if (pickLocation + sizeof(uint32) >= erasedLocation + s3_queue_size)
		{
			/* Insert location is advanced, but the area wasn't erased yet */
			return InvalidS3TaskLocation;
		}

		taskLen = *((uint32 *) (s3_queue_buffer + pickLocation % s3_queue_size));

		Assert((taskLen & LENGTH_ERASED_FLAG) == 0);

		if (taskLen == 0)
		{
			/* Insert location is advanced, but the data wasn't written yet */
			return InvalidS3TaskLocation;
		}

		/*
		 * Try to advance the pick location.  Whoever succeed on advancing the
		 * pick location is assumed to successfully pick the task.
		 */
		if (pg_atomic_compare_exchange_u64(&s3_queue_meta->pickLocation,
										   &pickLocation,
										   pickLocation + taskLen))
		{
			return pickLocation;
		}
	}
}

/*
 * Get the task by its location.
 */
Pointer
s3_queue_get_task(S3TaskLocation taskLocation)
{
	uint32		taskLen;
	Pointer		result;

	/* Get the task length */
	taskLen = *((uint32 *) (s3_queue_buffer + taskLocation % s3_queue_size));

	Assert(taskLen != 0);
	Assert((taskLen & LENGTH_ERASED_FLAG) == 0);

	result = (Pointer) palloc(taskLen - sizeof(uint32));

	/* Copy the task body */
	if (taskLocation / s3_queue_size == (taskLocation + taskLen - 1) / s3_queue_size)
	{
		/* Easy case: the task is a continuous chunk of memory */
		memcpy(result,
			   s3_queue_buffer + taskLocation % s3_queue_size + sizeof(uint32),
			   taskLen - sizeof(uint32));
	}
	else
	{
		/*
		 * More complex case: we hit the buffer end boundary.  In this case we
		 * have to assemble task from the two distinct chunks.
		 */
		uint32		firstChunkLen = s3_queue_size - taskLocation % s3_queue_size;

		Assert(firstChunkLen >= sizeof(uint32));

		memcpy(s3_queue_buffer + taskLocation % s3_queue_size + sizeof(uint32),
			   result,
			   firstChunkLen - sizeof(uint32));
		memcpy(result + (firstChunkLen - sizeof(uint32)),
			   s3_queue_buffer,
			   taskLen - firstChunkLen);
	}

	return result;
}

/*
 * Erase the processed task from the circular buffer.
 */
void
s3_queue_erase_task(S3TaskLocation taskLocation)
{
	uint32		taskLen;

	taskLen = *((uint32 *) (s3_queue_buffer + taskLocation % s3_queue_size));

	Assert(taskLen != 0);
	Assert((taskLen & LENGTH_ERASED_FLAG) == 0);

	/* Erase the task body */
	if (taskLocation / s3_queue_size == (taskLocation + taskLen - 1) / s3_queue_size)
	{
		/* Easy case: the task is a continuous chunk of memory */
		memset(s3_queue_buffer + taskLocation % s3_queue_size + sizeof(uint32),
			   0,
			   taskLen - sizeof(uint32));
	}
	else
	{
		/*
		 * More complex case: we hit the buffer end boundary.  In this case we
		 * have to erase the two distinct chunks.
		 */
		int			firstChunkLen = s3_queue_size - taskLen % s3_queue_size;

		Assert(firstChunkLen >= sizeof(uint32));

		memset(s3_queue_buffer + taskLocation % s3_queue_size + sizeof(uint32),
			   0,
			   firstChunkLen - sizeof(uint32));
		memset(s3_queue_buffer,
			   0,
			   taskLen - firstChunkLen);
	}

	pg_write_barrier();

	/* Put the LENGTH_ERASED_FLAG, which means we have erased the task body */
	*((uint32 *) (s3_queue_buffer + taskLocation % s3_queue_size)) = taskLen | LENGTH_ERASED_FLAG;

	/* Try to advance the erased location */
	while (pg_atomic_compare_exchange_u64(&s3_queue_meta->erasedLocation,
										  &taskLocation,
										  taskLocation + taskLen))
	{
		*((uint32 *) (s3_queue_buffer + taskLocation % s3_queue_size)) = 0;

		taskLocation += taskLen;

		/*
		 * Try to also advance erased location for the next task if
		 * appropriate.  It might happened that the next task is already
		 * erased but its process gave up on advancing the erased location. In
		 * this case we take a lead.  This algorithm guaranteed that somebody
		 * will advance the erased location anyway.
		 */
		taskLen = *((uint32 *) (s3_queue_buffer + taskLocation % s3_queue_size));
		if (!(taskLen & LENGTH_ERASED_FLAG))
			break;
		taskLen &= ~LENGTH_ERASED_FLAG;
	}

	ConditionVariableBroadcast(&s3_queue_meta->erasedLocationCV);
}

/*
 * Wait till the task with given location is processed by worker.
 */
void
s3_queue_wait_for_location(S3TaskLocation location)
{
	bool		slept = false;

	while (pg_atomic_read_u64(&s3_queue_meta->erasedLocation) <= location)
	{
		ConditionVariableSleep(&s3_queue_meta->erasedLocationCV,
							   WAIT_EVENT_MQ_PUT_MESSAGE);
		slept = true;
	}
	if (slept)
		ConditionVariableCancelSleep();
}
