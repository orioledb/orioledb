/*-------------------------------------------------------------------------
 *
 * headers.c
 * 		Routines for handling of S3-specific data file headers.
 *
 * Copyright (c) 2023, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/s3/headers.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <unistd.h>

#include "orioledb.h"

#include "btree/btree.h"
#include "btree/io.h"
#include "checkpoint/checkpoint.h"
#include "s3/headers.h"
#include "s3/worker.h"

#include "common/hashfn.h"
#include "pgstat.h"

#define S3_HEADER_BUFFERS_PER_GROUP 4
#define S3_HEADER_NUM_VALUES (ORIOLEDB_SEGMENT_SIZE / ORIOLEDB_S3_PART_SIZE)

typedef struct
{
	int			groupCtlTrancheId;
	int			bufferCtlTrancheId;
} S3HeadersMeta;

typedef struct
{
	LWLock		bufferCtlLock;
	S3HeaderTag tag;
	uint32		changeCount;
	uint32		usageCount;
	pg_atomic_uint64 data[S3_HEADER_NUM_VALUES];
} S3HeaderBuffer;

typedef struct
{
	LWLock		groupCtlLock;
	S3HeaderBuffer buffers[S3_HEADER_BUFFERS_PER_GROUP];
} S3HeadersBuffersGroup;

int			s3_headers_buffers_size;
static int	buffersCount = 0;
static int	groupsCount = 0;
static S3HeadersMeta *meta;
static S3HeadersBuffersGroup *groups;

#define S3_HEADER_MAX_CHANGE_COUNT (0x7FFFFFFF)

#define S3_PART_DIRTY_BIT		   UINT64CONST(0x8000000000000000)

#define S3_PART_CHANGE_COUNT_MASK  UINT64CONST(0x7FFFFFFF00000000)
#define S3_PART_CHANGE_COUNT_SHIFT (32)
#define S3_PART_GET_CHANGE_COUNT(p) (((p) & S3_PART_CHANGE_COUNT_MASK) >> S3_PART_CHANGE_COUNT_SHIFT)

#define S3_PART_LOWER_MASK		   UINT64CONST(0x00000000FFFFFFFF)
#define S3_PART_GET_LOWER(p)	   ((p) & S3_PART_LOWER_MASK)

#define S3_PART_MAKE(lower, changeCount, dirty) \
	((uint64) (lower) | \
	 ((uint64) (changeCount) << S3_PART_CHANGE_COUNT_SHIFT) | \
	 (uint64) ((dirty) ? S3_PART_DIRTY_BIT : 0))

#define S3_PART_LOCKS_NUM_MASK	   UINT64CONST(0x000000000003FFFF)
#define S3_PART_LOCKS_ONE		   (1)
#define S3_PART_LOCKS_NUM_SHIFT	   (0)
#define S3_PART_GET_LOCKS_NUM(p) (((p) & S3_PART_LOCKS_NUM_MASK) >> S3_PART_LOCKS_NUM_SHIFT)
#define S3_PART_USAGE_COUNT_MASK   UINT64CONST(0x00000000FF000000)
#define S3_PART_USAGE_COUNT_SHIFT  (24)
#define S3_PART_GET_USAGE_COUNT(p) (((p) & S3_PART_USAGE_COUNT_MASK) >> S3_PART_USAGE_COUNT_SHIFT)
#define S3_PART_DIRTY_FLAG		   UINT64CONST(0x0000000000800000)
#define S3_PART_STATUS_MASK		   UINT64CONST(0x0000000000700000)
#define S3_PART_STATUS_SHIFT	   (20)
#define S3_PART_GET_STATUS(p) ((S3PartStatus) (((p) & S3_PART_STATUS_MASK) >> S3_PART_STATUS_SHIFT))
#define S3_PART_SET_STATUS(p, s) (((p) & (~S3_PART_STATUS_MASK)) | ((uint64) s << S3_PART_STATUS_SHIFT))

Size
s3_headers_shmem_needs(void)
{
	buffersCount = (int) (((uint64) s3_headers_buffers_size * BLCKSZ) / ORIOLEDB_BLCKSZ);
	groupsCount = (buffersCount + S3_HEADER_BUFFERS_PER_GROUP - 1) / S3_HEADER_BUFFERS_PER_GROUP;

	return add_size(CACHELINEALIGN(sizeof(S3HeadersMeta)),
					CACHELINEALIGN(mul_size(sizeof(S3HeadersBuffersGroup), groupsCount)));
}

void
s3_headers_shmem_init(Pointer buf, bool found)
{
	Pointer		ptr = buf;

	meta = (S3HeadersMeta *) ptr;
	ptr += CACHELINEALIGN(sizeof(S3HeadersMeta));

	groups = (S3HeadersBuffersGroup *) ptr;

	if (!found)
	{
		uint32		i,
					j;

		meta->groupCtlTrancheId = LWLockNewTrancheId();
		meta->bufferCtlTrancheId = LWLockNewTrancheId();

		for (i = 0; i < groupsCount; i++)
		{
			S3HeadersBuffersGroup *group = &groups[i];

			LWLockInitialize(&group->groupCtlLock,
							 meta->groupCtlTrancheId);
			for (j = 0; j < S3_HEADER_BUFFERS_PER_GROUP; j++)
			{
				S3HeaderBuffer *buffer = &group->buffers[j];

				LWLockInitialize(&buffer->bufferCtlLock,
								 meta->bufferCtlTrancheId);
				buffer->tag.datoid = InvalidOid;
				buffer->tag.relnode = InvalidOid;
				buffer->tag.checkpointNum = 0;
				buffer->tag.segNum = 0;
				buffer->usageCount = 0;
				buffer->changeCount = 0;
			}
		}
	}
	LWLockRegisterTranche(meta->groupCtlTrancheId,
						  "S3HeadersGroupTranche");
	LWLockRegisterTranche(meta->bufferCtlTrancheId,
						  "S3HeadersBufferTranche");
}

static void
read_from_file(S3HeaderTag tag, uint32 values[S3_HEADER_NUM_VALUES],
			   bool *dirty)
{
	char	   *filename;
	File		file;
	int			headerSize = sizeof(uint32) * S3_HEADER_NUM_VALUES,
				rc;

	Assert(headerSize <= BLCKSZ);

	filename = btree_filename(tag.datoid, tag.relnode, tag.segNum,
							  tag.checkpointNum);
	file = PathNameOpenFile(filename, O_RDWR | O_CREAT | PG_BINARY);
	if (file <= 0)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not open data file %s", filename)));

	rc = OFileRead(file, (char *) values, headerSize, 0, WAIT_EVENT_DATA_FILE_READ);

	*dirty = false;
	if (rc == 0)
	{
		if (tag.checkpointNum <= checkpoint_state->lastCheckpointNumber)
		{
			MemSet(values, 0, headerSize);
		}
		else
		{
			int			i;

			for (i = 0; i < S3_HEADER_NUM_VALUES; i++)
				values[i] = S3_PART_SET_STATUS(0, S3PartStatusLoaded);
			*dirty = true;
		}
	}
	else if (rc != headerSize)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not read header from data file %s", filename)));

	FileClose(file);
}

static void
write_to_file(S3HeaderTag tag, uint32 values[S3_HEADER_NUM_VALUES])
{
	char	   *filename;
	File		file;
	int			headerSize = sizeof(uint32) * S3_HEADER_NUM_VALUES;

	Assert(headerSize <= BLCKSZ);

	filename = btree_filename(tag.datoid, tag.relnode, tag.segNum,
							  tag.checkpointNum);
	file = PathNameOpenFile(filename, O_RDWR | O_CREAT | PG_BINARY);
	if (file <= 0)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not open data file %s", filename)));

	if (OFileRead(file, (char *) values, headerSize, 0, WAIT_EVENT_DATA_FILE_READ) != headerSize)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not write header to data file %s", filename)));

	FileClose(file);
}

static void
load_header_buffer(S3HeaderTag tag)
{
	uint32		hash = hash_any((unsigned char *) &tag, sizeof(tag));
	S3HeadersBuffersGroup *group = &groups[hash % groupsCount];
	S3HeaderBuffer *buffer = NULL;
	int			i,
				victim = 0;
	uint32		victimUsageCount = 0;
	S3HeaderTag prevTag;
	uint32		oldValues[S3_HEADER_NUM_VALUES];
	uint32		newValues[S3_HEADER_NUM_VALUES];
	bool		dirty = false,
				newDirty = false;
	uint32		changeCount;

	/* First check if required buffer is already loaded */
	LWLockAcquire(&group->groupCtlLock, LW_EXCLUSIVE);

	/* Search for victim buffer */
	victim = 0;
	victimUsageCount = group->buffers[0].usageCount;
	for (i = 0; i < S3_HEADER_BUFFERS_PER_GROUP; i++)
	{
		buffer = &group->buffers[i];
		if (S3HeaderTagsIsEqual(buffer->tag, tag))
		{
			buffer->usageCount++;
			LWLockRelease(&group->groupCtlLock);
			return;
		}

		if (i == 0 || buffer->usageCount < victimUsageCount)
		{
			victim = i;
			victimUsageCount = buffer->usageCount;
		}
		buffer->usageCount /= 2;
	}

	buffer = &group->buffers[victim];
	LWLockAcquire(&buffer->bufferCtlLock, LW_EXCLUSIVE);

	prevTag = buffer->tag;

	buffer->usageCount++;
	if (buffer->changeCount == S3_HEADER_MAX_CHANGE_COUNT)
		buffer->changeCount = 0;
	else
		buffer->changeCount++;
	buffer->tag = tag;

	LWLockRelease(&group->groupCtlLock);

	changeCount = buffer->changeCount;

	read_from_file(tag, newValues, &newDirty);

	for (i = 0; i < S3_HEADER_NUM_VALUES; i++)
	{
		uint64		oldValue;

		oldValue = pg_atomic_exchange_u64(&buffer->data[i],
										  S3_PART_MAKE(newValues[i],
													   buffer->changeCount,
													   newDirty));
		Assert(S3_PART_GET_CHANGE_COUNT(oldValues[i]) == changeCount - 1);
		dirty = dirty || (oldValue & S3_PART_DIRTY_BIT);
		oldValues[i] = S3_PART_GET_LOWER(oldValue);
	}

	if (OidIsValid(prevTag.datoid) && OidIsValid(prevTag.relnode) && dirty)
		write_to_file(prevTag, oldValues);

	LWLockRelease(&buffer->bufferCtlLock);
}

static uint32
s3_header_read_value(S3HeaderTag tag, int index)
{
	uint32		hash = hash_any((unsigned char *) &tag, sizeof(tag));
	S3HeadersBuffersGroup *group = &groups[hash % groupsCount];
	int			i;

	while (true)
	{
		bool		tagMatched = false;

		for (i = 0; i < S3_HEADER_BUFFERS_PER_GROUP; i++)
		{
			S3HeaderBuffer *buffer = &group->buffers[i];

			if (S3HeaderTagsIsEqual(buffer->tag, tag))
			{
				/* check there is no read collision */
				uint32		changeCount = buffer->changeCount;
				uint64		value;

				pg_read_barrier();

				if (!S3HeaderTagsIsEqual(buffer->tag, tag))
					break;

				pg_read_barrier();

				if (buffer->changeCount != changeCount)
					break;

				value = pg_atomic_read_u64(&buffer->data[index]);

				if (S3_PART_GET_CHANGE_COUNT(value) != changeCount)
				{
					tagMatched = true;

					/*
					 * Change count mismatch, wait new page to be loaded.
					 */
					if (LWLockAcquireOrWait(&buffer->bufferCtlLock, LW_SHARED))
						LWLockRelease(&buffer->bufferCtlLock);
					break;
				}

				buffer->usageCount++;
				return S3_PART_GET_LOWER(value);
			}
		}

		if (!tagMatched)
			load_header_buffer(tag);
	}
}

static bool
s3_header_compare_and_swap(S3HeaderTag tag, int index,
						   uint32 *oldValue, uint32 newValue)
{
	uint32		hash = hash_any((unsigned char *) &tag, sizeof(tag));
	S3HeadersBuffersGroup *group = &groups[hash % groupsCount];
	int			i;

	while (true)
	{
		bool		tagMatched = false;

		for (i = 0; i < S3_HEADER_BUFFERS_PER_GROUP; i++)
		{
			S3HeaderBuffer *buffer = &group->buffers[i];

			if (S3HeaderTagsIsEqual(buffer->tag, tag))
			{
				/* check there is no read collision */
				uint32		changeCount = buffer->changeCount;
				uint64		fullValue;
				uint64		newFullValue;

				pg_read_barrier();

				if (!S3HeaderTagsIsEqual(buffer->tag, tag))
					break;

				pg_read_barrier();

				if (buffer->changeCount != changeCount)
					break;

				fullValue = pg_atomic_read_u64(&buffer->data[index]);

				if (S3_PART_GET_CHANGE_COUNT(fullValue) != changeCount)
				{
					tagMatched = true;

					/*
					 * Change count mismatch, wait new page to be loaded.
					 */
					if (LWLockAcquireOrWait(&buffer->bufferCtlLock, LW_SHARED))
						LWLockRelease(&buffer->bufferCtlLock);
					break;
				}

				if (S3_PART_GET_LOWER(fullValue) != *oldValue)
				{
					*oldValue = S3_PART_GET_LOWER(fullValue);
					return false;
				}

				newFullValue = S3_PART_MAKE(newValue, changeCount, true);

				if (pg_atomic_compare_exchange_u64(&buffer->data[index],
												   &fullValue, newFullValue))
				{
					buffer->usageCount++;
					return true;
				}
				else
				{
					*oldValue = S3_PART_GET_LOWER(fullValue);
					return false;
				}
			}
		}

		if (!tagMatched)
			load_header_buffer(tag);
	}
}

/*
 * We allow only one part to be locked simultaneosly.
 */
static S3HeaderTag curLockedTag = {InvalidOid, InvalidOid, 0, 0};
static int	curLockedIndex = 0;

/*
 * Lock file part in S3 header.  This shouldn't let anybody to concurrently
 * evict the same file part.
 */
bool
s3_header_lock_part(S3HeaderTag tag, int index)
{
	uint32		value;

	Assert(!OidIsValid(curLockedTag.datoid) && !OidIsValid(curLockedTag.relnode));

	value = s3_header_read_value(tag, index);

	while (true)
	{
		uint32		newValue = value;
		S3PartStatus status;

		status = S3_PART_GET_STATUS(value);

		if (status == S3PartStatusNotLoaded)
		{
			s3_load_file_part(tag.checkpointNum, tag.datoid,
							  tag.relnode, tag.segNum, index);
		}
		else if (status == S3PartStatusLoading)
		{
			while (S3_PART_GET_STATUS(value) == S3PartStatusLoading)
			{
				pg_usleep(10000L);
				value = s3_header_read_value(tag, index);
			}
			continue;
		}
		else
		{
			Assert(status == S3PartStatusLoaded);
			newValue += S3_PART_LOCKS_ONE;
		}

		if (s3_header_compare_and_swap(tag, index, &value, newValue))
		{
			curLockedTag = tag;
			curLockedIndex = index;
			return (value & S3_PART_DIRTY_FLAG);
		}
	}
}

S3PartStatus
s3_header_mark_part_loading(S3HeaderTag tag, int index)
{
	uint32		value;

	value = s3_header_read_value(tag, index);

	while (true)
	{
		uint32		newValue;
		S3PartStatus status;

		status = S3_PART_GET_STATUS(value);

		if (status == S3PartStatusLoaded || status == S3PartStatusLoading)
		{
			return false;
		}
		else
		{
			Assert(status == S3PartStatusNotLoaded);
			newValue = S3_PART_SET_STATUS(value, S3PartStatusLoading);
		}

		if (s3_header_compare_and_swap(tag, index, &value, newValue))
			return status;
	}
}

void
s3_header_mark_part_loaded(S3HeaderTag tag, int index)
{
	uint32		value;

	value = s3_header_read_value(tag, index);

	while (true)
	{
		uint32		newValue;
		S3PartStatus status;

		status = S3_PART_GET_STATUS(value);

		Assert(status == S3PartStatusLoading);
		newValue = S3_PART_SET_STATUS(value, S3PartStatusLoaded);

		if (s3_header_compare_and_swap(tag, index, &value, newValue))
			return;
	}
}

void
s3_header_unlock_part(S3HeaderTag tag, int index, bool setDirty)
{
	uint32		value;

	value = s3_header_read_value(tag, index);

	while (true)
	{
		uint32		newValue = value;

		Assert(S3_PART_GET_STATUS(value) == S3PartStatusLoaded);
		Assert(S3_PART_GET_LOCKS_NUM(value) >= 0);

		newValue -= S3_PART_LOCKS_ONE;
		if (setDirty)
			newValue |= S3_PART_DIRTY_FLAG;

		if (s3_header_compare_and_swap(tag, index, &value, newValue))
		{
			curLockedTag.datoid = InvalidOid;
			curLockedTag.relnode = InvalidOid;
			curLockedTag.checkpointNum = 0;
			curLockedTag.segNum = 0;
			curLockedIndex = 0;
			return;
		}
	}
}

void
s3_headers_error_cleanup(void)
{
	if (!OidIsValid(curLockedTag.datoid) || !OidIsValid(curLockedTag.relnode))
		return;

	s3_header_unlock_part(curLockedTag, curLockedIndex, false);
}
