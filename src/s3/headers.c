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

#include "common/file_perm.h"
#include "common/file_utils.h"
#include "common/hashfn.h"
#include "common/pg_prng.h"
#if PG_VERSION_NUM < 160000
#include "port/pg_iovec.h"
#endif
#include "pgstat.h"
#if PG_VERSION_NUM < 160000
#include "storage/fd.h"
#endif

#define S3_HEADER_BUFFERS_PER_GROUP 4
#define S3_HEADER_NUM_VALUES (ORIOLEDB_SEGMENT_SIZE / ORIOLEDB_S3_PART_SIZE)

typedef struct
{
	int			groupCtlTrancheId;
	int			bufferCtlTrancheId;
	pg_atomic_uint64 numberOfLoadedParts;
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
#define S3_PART_SET_USAGE_COUNT(p, u) (((p) & (~S3_PART_USAGE_COUNT_MASK)) | ((uint64) (u) << S3_PART_USAGE_COUNT_SHIFT))
#define S3_PART_DIRTY_FLAG		   UINT64CONST(0x0000000000800000)
#define S3_PART_STATUS_MASK		   UINT64CONST(0x0000000000700000)
#define S3_PART_STATUS_SHIFT	   (20)
#define S3_PART_GET_STATUS(p) ((S3PartStatus) (((p) & S3_PART_STATUS_MASK) >> S3_PART_STATUS_SHIFT))
#define S3_PART_SET_STATUS(p, s) (((p) & (~S3_PART_STATUS_MASK)) | ((uint64) (s) << S3_PART_STATUS_SHIFT))

static void initial_parts_conting(void);
static void sync_buffer(S3HeaderBuffer *buffer);

#if PG_VERSION_NUM < 160000
/*
 * pg_pwrite_zeros
 *
 * Writes zeros to file worth "size" bytes at "offset" (from the start of the
 * file), using vectored I/O.
 *
 * Returns the total amount of data written.  On failure, a negative value
 * is returned with errno set.
 */
static ssize_t
pg_pwrite_zeros(int fd, size_t size, off_t offset)
{
	static const PGAlignedBlock zbuffer = {{0}};	/* worth BLCKSZ */
	void	   *zerobuf_addr = unconstify(PGAlignedBlock *, &zbuffer)->data;
	struct iovec iov[PG_IOV_MAX];
	size_t		remaining_size = size;
	ssize_t		total_written = 0;

	/* Loop, writing as many blocks as we can for each system call. */
	while (remaining_size > 0)
	{
		int			iovcnt = 0;
		ssize_t		written;

		for (; iovcnt < PG_IOV_MAX && remaining_size > 0; iovcnt++)
		{
			size_t		this_iov_size;

			iov[iovcnt].iov_base = zerobuf_addr;

			if (remaining_size < BLCKSZ)
				this_iov_size = remaining_size;
			else
				this_iov_size = BLCKSZ;

			iov[iovcnt].iov_len = this_iov_size;
			remaining_size -= this_iov_size;
		}

		written = pg_pwritev_with_retry(fd, iov, iovcnt, offset);

		if (written < 0)
			return written;

		offset += written;
		total_written += written;
	}

	Assert(total_written == size);

	return total_written;
}
#endif

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
		pg_atomic_init_u64(&meta->numberOfLoadedParts, 0);

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

	if (orioledb_s3_mode)
		initial_parts_conting();
}

void
s3_headers_increase_loaded_parts(uint64 inc)
{
	(void) pg_atomic_fetch_add_u64(&meta->numberOfLoadedParts, inc);
}

static void
read_from_file(S3HeaderTag tag, uint32 values[S3_HEADER_NUM_VALUES],
			   bool *dirty)
{
	char	   *filename;
	int			fd;
	int			headerSize = sizeof(uint32) * S3_HEADER_NUM_VALUES,
				rc;

	Assert(headerSize <= BLCKSZ);

	filename = btree_filename(tag.datoid, tag.relnode, tag.segNum,
							  tag.checkpointNum);
	fd = BasicOpenFilePerm(filename, O_RDWR | O_CREAT | PG_BINARY, pg_file_create_mode);
	if (fd <= 0)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not open data file %s", filename)));

	pgstat_report_wait_start(WAIT_EVENT_DATA_FILE_READ);
	rc = pg_pread(fd, (char *) values, headerSize, 0);
	pgstat_report_wait_end();

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

	close(fd);
}

static void
write_to_file(S3HeaderTag tag, uint32 values[S3_HEADER_NUM_VALUES])
{
	char	   *filename;
	int			fd;
	int			headerSize = sizeof(uint32) * S3_HEADER_NUM_VALUES;

	Assert(headerSize <= BLCKSZ);

	filename = btree_filename(tag.datoid, tag.relnode, tag.segNum,
							  tag.checkpointNum);
	fd = BasicOpenFilePerm(filename, O_RDWR | O_CREAT | PG_BINARY, pg_file_create_mode);
	if (fd <= 0)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not open data file %s", filename)));

	pgstat_report_wait_start(WAIT_EVENT_DATA_FILE_WRITE);
	if (pg_pwrite(fd, (char *) values, headerSize, 0) != headerSize)
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not write file \"%s\": %m", filename)));
	pg_flush_data(fd, 0, headerSize);
	pgstat_report_wait_end();

	close(fd);
}

static void
change_buffer(S3HeadersBuffersGroup *group, int index, S3HeaderTag tag)
{
	S3HeaderTag prevTag;
	uint32		oldValues[S3_HEADER_NUM_VALUES];
	uint32		newValues[S3_HEADER_NUM_VALUES];
	bool		dirty = false,
				newDirty = false;
	uint32		prevChangeCount PG_USED_FOR_ASSERTS_ONLY;
	S3HeaderBuffer *buffer = NULL;
	int			i;
	bool		haveLoadedPart = false;
	bool		checkUnlink = false;
	off_t		prevFileSize = ORIOLEDB_SEGMENT_SIZE;

	buffer = &group->buffers[index];

	prevTag = buffer->tag;
	prevChangeCount = buffer->changeCount;

	if (buffer->changeCount == S3_HEADER_MAX_CHANGE_COUNT)
		buffer->changeCount = 0;
	else
		buffer->changeCount++;
	buffer->tag = tag;

	LWLockRelease(&group->groupCtlLock);


	if (OidIsValid(tag.datoid) && OidIsValid(tag.relnode))
	{
		read_from_file(tag, newValues, &newDirty);
	}
	else
	{
		memset(newValues, 0, sizeof(uint32) * S3_HEADER_NUM_VALUES);
		checkUnlink = true;
	}

	if (checkUnlink)
	{
		char	   *filename;
		int			fd;

		filename = btree_filename(prevTag.datoid, prevTag.relnode,
								  prevTag.segNum, prevTag.checkpointNum);
		fd = BasicOpenFilePerm(filename, O_RDWR | PG_BINARY, pg_file_create_mode);
		pfree(filename);
		if (fd > 0)
		{
			prevFileSize = lseek(fd, 0, SEEK_END);
			close(fd);
		}
	}

	for (i = 0; i < S3_HEADER_NUM_VALUES; i++)
	{
		uint64		oldValue;

		oldValue = pg_atomic_exchange_u64(&buffer->data[i],
										  S3_PART_MAKE(newValues[i],
													   buffer->changeCount,
													   newDirty));
		if (S3_PART_GET_STATUS(oldValue) != S3PartStatusNotLoaded &&
			(uint64) i * (uint64) ORIOLEDB_S3_PART_SIZE < prevFileSize)
			haveLoadedPart = true;
		Assert(S3_PART_GET_CHANGE_COUNT(oldValue) == prevChangeCount);
		dirty = dirty || (oldValue & S3_PART_DIRTY_BIT);
		oldValues[i] = S3_PART_GET_LOWER(oldValue);
	}

	if (checkUnlink && !haveLoadedPart)
	{
		char	   *filename;

		filename = btree_filename(prevTag.datoid, prevTag.relnode,
								  prevTag.segNum, prevTag.checkpointNum);
		unlink(filename);
		pfree(filename);
	}
	else if (OidIsValid(prevTag.datoid) && OidIsValid(prevTag.relnode) && dirty)
	{
		write_to_file(prevTag, oldValues);
	}

	LWLockRelease(&buffer->bufferCtlLock);

}

static void
load_header_buffer(S3HeaderTag tag)
{
	uint32		hash = hash_any((unsigned char *) &tag, sizeof(tag));
	S3HeadersBuffersGroup *group = &groups[hash % groupsCount];
	int			i,
				victim = 0;
	uint32		victimUsageCount = 0;

	/* First check if required buffer is already loaded */
	LWLockAcquire(&group->groupCtlLock, LW_EXCLUSIVE);

	/* Search for victim buffer */
	victim = 0;
	victimUsageCount = group->buffers[0].usageCount;
	for (i = 0; i < S3_HEADER_BUFFERS_PER_GROUP; i++)
	{
		S3HeaderBuffer *buffer = &group->buffers[i];

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

	LWLockAcquire(&group->buffers[victim].bufferCtlLock, LW_EXCLUSIVE);

	change_buffer(group, victim, tag);

}

static void
check_unlink_file(S3HeaderTag tag)
{
	uint32		hash = hash_any((unsigned char *) &tag, sizeof(tag));
	S3HeadersBuffersGroup *group = &groups[hash % groupsCount];
	int			victim = 0;
	S3HeaderTag newTag;

	while (true)
	{
		bool		found = false;

		/* First check if required buffer is already loaded */
		LWLockAcquire(&group->groupCtlLock, LW_EXCLUSIVE);
		for (victim = 0; victim < S3_HEADER_BUFFERS_PER_GROUP; victim++)
		{
			S3HeaderBuffer *buffer = &group->buffers[victim];

			if (S3HeaderTagsIsEqual(buffer->tag, tag))
			{
				found = true;
				break;
			}
		}
		if (found)
			break;
		LWLockRelease(&group->groupCtlLock);
		load_header_buffer(tag);
	}

	Assert(victim < S3_HEADER_BUFFERS_PER_GROUP);
	/* if added because of cppcheck */
	if (victim < S3_HEADER_BUFFERS_PER_GROUP)
	{
		LWLockAcquire(&group->buffers[victim].bufferCtlLock, LW_EXCLUSIVE);
		newTag.datoid = InvalidOid;
		newTag.relnode = InvalidOid;
		newTag.checkpointNum = 0;
		newTag.segNum = 0;
		change_buffer(group, victim, newTag);
	}
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
					if (S3_PART_GET_STATUS(fullValue) == S3PartStatusLoaded &&
						S3_PART_GET_STATUS(newFullValue) == S3PartStatusNotLoaded)
						sync_buffer(buffer);
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
		uint32		newValue = value,
					usageCount = S3_PART_GET_USAGE_COUNT(value);
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
			newValue = S3_PART_SET_USAGE_COUNT(newValue, usageCount + 1);
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
		{
			(void) pg_atomic_fetch_add_u64(&meta->numberOfLoadedParts, 1);
			return status;
		}
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
		S3PartStatus status PG_USED_FOR_ASSERTS_ONLY;

		status = S3_PART_GET_STATUS(value);

		Assert(status == S3PartStatusLoading);
		newValue = S3_PART_SET_STATUS(value, S3PartStatusLoaded);
		newValue = S3_PART_SET_USAGE_COUNT(newValue, 1);

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

static void
sync_buffer(S3HeaderBuffer *buffer)
{
	S3HeaderTag tag;
	uint32		oldValues[S3_HEADER_NUM_VALUES];
	bool		dirty = false;
	int			i;

	LWLockAcquire(&buffer->bufferCtlLock, LW_EXCLUSIVE);

	for (i = 0; i < S3_HEADER_NUM_VALUES; i++)
	{
		uint64		oldValue;

		oldValue = pg_atomic_fetch_and_u64(&buffer->data[i],
										   ~S3_PART_DIRTY_BIT);
		dirty = dirty || (oldValue & S3_PART_DIRTY_BIT);
		oldValues[i] = S3_PART_GET_LOWER(oldValue);
	}

	tag = buffer->tag;
	if (OidIsValid(tag.datoid) && OidIsValid(tag.relnode) && dirty)
		write_to_file(tag, oldValues);

	LWLockRelease(&buffer->bufferCtlLock);
}

void
s3_headers_sync(void)
{
	int			i,
				j;

	for (i = 0; i < groupsCount; i++)
	{
		for (j = 0; j < S3_HEADER_BUFFERS_PER_GROUP; j++)
			sync_buffer(&groups[i].buffers[j]);
	}
}

void
s3_headers_error_cleanup(void)
{
	if (!OidIsValid(curLockedTag.datoid) || !OidIsValid(curLockedTag.relnode))
		return;

	s3_header_unlock_part(curLockedTag, curLockedIndex, false);
}

typedef void (*IterateFilesCallback) (S3HeaderTag tag);

static void
iterate_files(IterateFilesCallback callback)
{
	DIR		   *dir,
			   *dbDir;
	struct dirent *file,
			   *dbFile;

	dir = opendir(ORIOLEDB_DATA_DIR);
	if (dir == NULL)
		ereport(PANIC, (errcode_for_file_access(),
						errmsg("could not open the orioledb data directory")));

	while (errno = 0, (file = readdir(dir)) != NULL)
	{
		Oid			dbOid;
		char	   *dbDirName;

		if (sscanf(file->d_name, "%u", &dbOid) != 1)
			continue;

		dbDirName = psprintf(ORIOLEDB_DATA_DIR "/%u", dbOid);
		dbDir = opendir(dbDirName);
		pfree(dbDirName);
		if (dbDir == NULL)
			continue;

		while (errno = 0, (dbFile = readdir(dbDir)) != NULL)
		{
			uint32		file_reloid,
						file_chkp,
						file_segno;
			S3HeaderTag tag;

			if (sscanf(dbFile->d_name, "%10u-%10u",
					   &file_reloid, &file_chkp) == 2)
			{
				tag.checkpointNum = file_chkp;
				tag.datoid = dbOid;
				tag.relnode = file_reloid;
				tag.segNum = 0;
				callback(tag);
			}
			else if (sscanf(dbFile->d_name, "%10u.%10u-%10u",
							&file_reloid, &file_segno, &file_chkp) == 3)
			{
				tag.checkpointNum = file_chkp;
				tag.datoid = dbOid;
				tag.relnode = file_reloid;
				tag.segNum = file_segno;
				callback(tag);
			}
		}
		closedir(dbDir);
	}

	closedir(dir);
}

static off_t totalFilesSize;
static off_t totalOccupiedSize;
static uint64 totalFilesCount;

static void
initial_parts_counting_callback(S3HeaderTag tag)
{
	uint32		values[S3_HEADER_NUM_VALUES];
	bool		dirty;
	off_t		fileSize;
	int			i;
	int			fd;
	char	   *filename;

	read_from_file(tag, values, &dirty);

	filename = btree_filename(tag.datoid, tag.relnode, tag.segNum,
							  tag.checkpointNum);
	fd = BasicOpenFilePerm(filename, O_RDWR | PG_BINARY, pg_file_create_mode);
	if (fd <= 0)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not open data file %s", filename)));
	pfree(filename);

	fileSize = lseek(fd, 0, SEEK_END);
	totalFilesCount++;
	totalFilesSize += fileSize;

	for (i = 0; i < S3_HEADER_NUM_VALUES; i++)
	{
		S3PartStatus status = S3_PART_GET_STATUS(values[i]);
		uint32		usageCount = S3_PART_GET_USAGE_COUNT(values[i]);
		off_t		offset = (off_t) i * (off_t) ORIOLEDB_S3_PART_SIZE + (off_t) ORIOLEDB_BLCKSZ;

		if (status == S3PartStatusNotLoaded || status == S3PartStatusLoading)
		{

			status = S3PartStatusNotLoaded;
			if (fileSize > offset)
			{
				pgstat_report_wait_start(WAIT_EVENT_DATA_FILE_WRITE);
				pg_pwrite_zeros(fd, Min(offset + ORIOLEDB_S3_PART_SIZE, fileSize) - offset, offset);
				pgstat_report_wait_end();
			}
		}
		else
		{
			if (fileSize > offset)
			{
				(void) pg_atomic_fetch_add_u64(&meta->numberOfLoadedParts, 1);
				totalOccupiedSize += Min(ORIOLEDB_S3_PART_SIZE, fileSize - offset);
			}
		}
		values[i] = S3_PART_SET_STATUS(usageCount << S3_PART_USAGE_COUNT_SHIFT, status);
	}

	close(fd);

	write_to_file(tag, values);
}

static void
initial_parts_conting(void)
{
	totalFilesSize = 0;
	totalOccupiedSize = 0;
	totalFilesCount = 0;
	elog(LOG, "OrioleDB initial files scan started");
	iterate_files(initial_parts_counting_callback);
	elog(LOG, "OrioleDB initial files scan finished (num files %llu, size %llu, occupied size %llu)",
		 (unsigned long long) totalFilesCount,
		 (unsigned long long) totalFilesSize,
		 (unsigned long long) totalOccupiedSize);

}

static void
eviction_callback(S3HeaderTag tag)
{
	int			fd;
	char	   *filename;
	off_t		fileSize;
	int			i;
	int			numParts;
	bool		haveLoadedParts = false;

	if (tag.checkpointNum > checkpoint_state->lastCheckpointNumber)
		return;

	filename = btree_filename(tag.datoid, tag.relnode, tag.segNum,
							  tag.checkpointNum);
	fd = BasicOpenFilePerm(filename, O_RDWR | PG_BINARY, pg_file_create_mode);
	pfree(filename);

	if (fd <= 0)
		return;

	fileSize = lseek(fd, 0, SEEK_END);

	numParts = (fileSize + ORIOLEDB_S3_PART_SIZE - 1) / ORIOLEDB_S3_PART_SIZE;
	for (i = 0; i < numParts; i++)
	{
		uint32		value;

		if (i == numParts - 1 && fileSize < (uint64) numParts * (uint64) ORIOLEDB_S3_PART_SIZE)
		{
			static pg_prng_state random_state;
			static bool seed_initialized = false;

			if (!seed_initialized)
			{
				pg_prng_seed(&random_state, 0);
				seed_initialized = true;
			}

			if (pg_prng_int32(&random_state) % ORIOLEDB_S3_PART_SIZE >
				fileSize - (uint64) (numParts - 1) * (uint64) ORIOLEDB_S3_PART_SIZE)
			{
				continue;
			}
		}

		value = s3_header_read_value(tag, i);

		while (true)
		{
			uint32		newValue = value,
						usageCount = S3_PART_GET_USAGE_COUNT(value);

			if (S3_PART_GET_STATUS(value) == S3PartStatusLoaded &&
				S3_PART_GET_LOCKS_NUM(value) == 0 && usageCount == 0)
			{
				newValue = S3_PART_SET_STATUS(newValue, S3PartStatusNotLoaded);
			}
			else
			{
				newValue = S3_PART_SET_USAGE_COUNT(newValue, usageCount / 2);
			}

			if (s3_header_compare_and_swap(tag, i, &value, newValue))
			{
				if (S3_PART_GET_STATUS(value) == S3PartStatusLoaded &&
					S3_PART_GET_STATUS(newValue) == S3PartStatusNotLoaded)
				{
					off_t		offset = (off_t) i * (off_t) ORIOLEDB_S3_PART_SIZE + (off_t) ORIOLEDB_BLCKSZ;

					pg_pwrite_zeros(fd, Min(offset + ORIOLEDB_S3_PART_SIZE, fileSize) - offset, offset);
					(void) pg_atomic_fetch_sub_u64(&meta->numberOfLoadedParts, 1);
				}
				if (S3_PART_GET_STATUS(newValue) != S3PartStatusNotLoaded)
					haveLoadedParts = true;
				break;
			}
		}
	}

	if (!haveLoadedParts)
	{
		check_unlink_file(tag);
	}

	close(fd);
}

void
s3_headers_try_eviction_cycle(void)
{
	uint64		desiredNumParts = (uint64) s3_desired_size * (uint64) (1024 * 1024) / (uint64) ORIOLEDB_S3_PART_SIZE;

	Assert(orioledb_s3_mode);

	if (pg_atomic_read_u64(&meta->numberOfLoadedParts) < desiredNumParts)
		return;

	iterate_files(eviction_callback);

}
