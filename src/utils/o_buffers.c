/*-------------------------------------------------------------------------
 *
 * o_buffers.c
 * 		Buffering layer for file access.
 *
 * Copyright (c) 2021-2025, Oriole DB Inc.
 * Copyright (c) 2025, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/utils/o_buffers.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <unistd.h>

#include "orioledb.h"

#include "btree/btree.h"
#include "btree/io.h"
#include "utils/o_buffers.h"

#include "pgstat.h"

#define O_BUFFERS_PER_GROUP 4

struct OBuffersMeta
{
	int			groupCtlTrancheId;
	int			bufferCtlTrancheId;
};

typedef struct
{
	LWLock		bufferCtlLock;
	int64		blockNum;
	int64		shadowBlockNum;
	uint32		tag;
	uint32		shadowTag;
	uint32		usageCount;
	bool		dirty;
	char		data[ORIOLEDB_BLCKSZ];
} OBuffer;

struct OBuffersGroup
{
	LWLock		groupCtlLock;
	OBuffer		buffers[O_BUFFERS_PER_GROUP];
};

Size
o_buffers_shmem_needs(OBuffersDesc *desc)
{
	desc->groupsCount = (desc->buffersCount + O_BUFFERS_PER_GROUP - 1) / O_BUFFERS_PER_GROUP;

	return add_size(CACHELINEALIGN(sizeof(OBuffersMeta)),
					CACHELINEALIGN(mul_size(sizeof(OBuffersGroup), desc->groupsCount)));
}

void
o_buffers_shmem_init(OBuffersDesc *desc, void *buf, bool found)
{
	Pointer		ptr = buf;

	desc->metaPageBlkno = (OBuffersMeta *) ptr;
	ptr += CACHELINEALIGN(sizeof(OBuffersMeta));

	desc->groups = (OBuffersGroup *) ptr;
	desc->groupsCount = (desc->buffersCount + O_BUFFERS_PER_GROUP - 1) / O_BUFFERS_PER_GROUP;
	desc->curFile = -1;

	Assert((desc->singleFileSize % ORIOLEDB_BLCKSZ) == 0);

	if (!found)
	{
		uint32		i,
					j;

		desc->metaPageBlkno->groupCtlTrancheId = LWLockNewTrancheId();
		desc->metaPageBlkno->bufferCtlTrancheId = LWLockNewTrancheId();

		for (i = 0; i < desc->groupsCount; i++)
		{
			OBuffersGroup *group = &desc->groups[i];

			LWLockInitialize(&group->groupCtlLock,
							 desc->metaPageBlkno->groupCtlTrancheId);
			for (j = 0; j < O_BUFFERS_PER_GROUP; j++)
			{
				OBuffer    *buffer = &group->buffers[j];

				LWLockInitialize(&buffer->bufferCtlLock,
								 desc->metaPageBlkno->bufferCtlTrancheId);
				buffer->blockNum = -1;
				buffer->usageCount = 0;
				buffer->dirty = false;
				buffer->tag = 0;
			}
		}
	}
	LWLockRegisterTranche(desc->metaPageBlkno->groupCtlTrancheId,
						  desc->groupCtlTrancheName);
	LWLockRegisterTranche(desc->metaPageBlkno->bufferCtlTrancheId,
						  desc->bufferCtlTrancheName);
}

static void
open_file(OBuffersDesc *desc, uint32 tag, uint64 fileNum)
{
	Assert(OBuffersMaxTagIsValid(tag));

	if (desc->curFile >= 0 &&
		desc->curFileNum == fileNum &&
		desc->curFileTag == tag)
		return;

	if (desc->curFile >= 0)
		FileClose(desc->curFile);

	pg_snprintf(desc->curFileName, MAXPGPATH,
				desc->filenameTemplate[tag],
				(uint32) (fileNum >> 32),
				(uint32) fileNum);
	desc->curFile = PathNameOpenFile(desc->curFileName,
									 O_RDWR | O_CREAT | PG_BINARY);
	desc->curFileNum = fileNum;
	desc->curFileTag = tag;
	if (desc->curFile < 0)
		ereport(PANIC, (errcode_for_file_access(),
						errmsg("could not open undo log file %s: %m", desc->curFileName)));
}

static void
unlink_file(OBuffersDesc *desc, uint32 tag, uint64 fileNum)
{
	static char fileNameToUnlink[MAXPGPATH];

	Assert(OBuffersMaxTagIsValid(tag));

	pg_snprintf(fileNameToUnlink, MAXPGPATH,
				desc->filenameTemplate[tag],
				(uint32) (fileNum >> 32),
				(uint32) fileNum);

	(void) unlink(fileNameToUnlink);
}

static void
write_buffer_data(OBuffersDesc *desc, char *data, uint32 tag, uint64 blockNum)
{
	int			result;

	Assert(OBuffersMaxTagIsValid(tag));

	open_file(desc, tag, blockNum / (desc->singleFileSize / ORIOLEDB_BLCKSZ));
	result = OFileWrite(desc->curFile, data, ORIOLEDB_BLCKSZ,
						(blockNum * ORIOLEDB_BLCKSZ) % desc->singleFileSize,
						WAIT_EVENT_SLRU_WRITE);
	if (result != ORIOLEDB_BLCKSZ)
		ereport(PANIC, (errcode_for_file_access(),
						errmsg("could not write buffer to file %s: %m", desc->curFileName)));
}

static void
write_buffer(OBuffersDesc *desc, OBuffer *buffer)
{
	write_buffer_data(desc, buffer->data, buffer->tag, buffer->blockNum);
}

static void
read_buffer(OBuffersDesc *desc, OBuffer *buffer)
{
	int			result;

	open_file(desc, buffer->tag,
			  buffer->blockNum / (desc->singleFileSize / ORIOLEDB_BLCKSZ));
	result = OFileRead(desc->curFile, buffer->data, ORIOLEDB_BLCKSZ,
					   (buffer->blockNum * ORIOLEDB_BLCKSZ) % desc->singleFileSize,
					   WAIT_EVENT_SLRU_READ);

	/* we may not read all the bytes due to read past EOF */
	if (result < 0)
		ereport(PANIC, (errcode_for_file_access(),
						errmsg("could not read buffer from file %s: %m", desc->curFileName)));

	if (result < ORIOLEDB_BLCKSZ)
		memset(&buffer->data[result], 0, ORIOLEDB_BLCKSZ - result);
}

static OBuffer *
get_buffer(OBuffersDesc *desc, uint32 tag, int64 blockNum, bool write)
{
	OBuffersGroup *group = &desc->groups[blockNum % desc->groupsCount];
	OBuffer    *buffer = NULL;
	int			i,
				victim = 0;
	uint32		victimUsageCount = 0;
	bool		prevDirty;
	int64		prevBlockNum;
	uint32		prevTag;

	/* First check if required buffer is already loaded */
	LWLockAcquire(&group->groupCtlLock, LW_SHARED);
	for (i = 0; i < O_BUFFERS_PER_GROUP; i++)
	{
		buffer = &group->buffers[i];
		if (buffer->blockNum == blockNum &&
			buffer->tag == tag)
		{
			LWLockAcquire(&buffer->bufferCtlLock, write ? LW_EXCLUSIVE : LW_SHARED);
			buffer->usageCount++;
			LWLockRelease(&group->groupCtlLock);

			return buffer;
		}
	}
	LWLockRelease(&group->groupCtlLock);

	/* No luck: have to evict some buffer */
	LWLockAcquire(&group->groupCtlLock, LW_EXCLUSIVE);

	/* Search for victim buffer */
	for (i = 0; i < O_BUFFERS_PER_GROUP; i++)
	{
		buffer = &group->buffers[i];

		/* Need to recheck after relock */
		if (buffer->blockNum == blockNum &&
			buffer->tag == tag)
		{
			LWLockAcquire(&buffer->bufferCtlLock, write ? LW_EXCLUSIVE : LW_SHARED);
			buffer->usageCount++;
			LWLockRelease(&group->groupCtlLock);

			return buffer;
		}

		if (buffer->shadowBlockNum == blockNum &&
			buffer->shadowTag == tag)
		{
			/*
			 * There is an in-progress operation with required tag.  We must
			 * wait till it's completed.
			 */
			if (LWLockAcquireOrWait(&buffer->bufferCtlLock, LW_SHARED))
				LWLockRelease(&buffer->bufferCtlLock);
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

	prevDirty = buffer->dirty;
	prevBlockNum = buffer->blockNum;
	prevTag = buffer->tag;

	buffer->usageCount = 1;
	buffer->dirty = false;
	buffer->blockNum = blockNum;
	buffer->tag = tag;
	buffer->shadowBlockNum = prevBlockNum;
	buffer->shadowTag = prevTag;

	LWLockRelease(&group->groupCtlLock);

	if (prevDirty)
		write_buffer_data(desc, buffer->data, prevTag, prevBlockNum);

	read_buffer(desc, buffer);

	buffer->shadowBlockNum = -1;

	return buffer;
}

static void
o_buffers_rw(OBuffersDesc *desc, Pointer buf,
			 uint32 tag, int64 offset, int64 size,
			 bool write)
{
	int64		firstBlockNum = offset / ORIOLEDB_BLCKSZ,
				lastBlockNum = (offset + size - 1) / ORIOLEDB_BLCKSZ,
				blockNum;
	Pointer		ptr = buf;

	for (blockNum = firstBlockNum; blockNum <= lastBlockNum; blockNum++)
	{
		OBuffer    *buffer = get_buffer(desc, tag, blockNum, write);
		uint32		copySize,
					copyOffset;

		if (firstBlockNum == lastBlockNum)
		{
			copySize = size;
			copyOffset = offset % ORIOLEDB_BLCKSZ;
		}
		else if (blockNum == firstBlockNum)
		{
			copySize = ORIOLEDB_BLCKSZ - offset % ORIOLEDB_BLCKSZ;
			copyOffset = offset % ORIOLEDB_BLCKSZ;
		}
		else if (blockNum == lastBlockNum)
		{
			copySize = (offset + size - 1) % ORIOLEDB_BLCKSZ + 1;
			copyOffset = 0;
		}
		else
		{
			copySize = ORIOLEDB_BLCKSZ;
			copyOffset = 0;
		}

		if (write)
		{
			memcpy(&buffer->data[copyOffset], ptr, copySize);
			buffer->dirty = true;
		}
		else
		{
			memcpy(ptr, &buffer->data[copyOffset], copySize);
		}
		ptr += copySize;
		LWLockRelease(&buffer->bufferCtlLock);
	}
}

void
o_buffers_read(OBuffersDesc *desc, Pointer buf, uint32 tag, int64 offset, int64 size)
{
	Assert(OBuffersMaxTagIsValid(tag) && offset >= 0 && size > 0);
	o_buffers_rw(desc, buf, tag, offset, size, false);
}

void
o_buffers_write(OBuffersDesc *desc, Pointer buf, uint32 tag, int64 offset, int64 size)
{
	Assert(OBuffersMaxTagIsValid(tag) && offset >= 0 && size > 0);
	o_buffers_rw(desc, buf, tag, offset, size, true);
}

static void
o_buffers_flush(OBuffersDesc *desc,
				uint32 tag,
				int64 firstBufferNumber,
				int64 lastBufferNumber)
{
	int			i,
				j;

	for (i = 0; i < desc->groupsCount; i++)
	{
		OBuffersGroup *group = &desc->groups[i];

		for (j = 0; j < O_BUFFERS_PER_GROUP; j++)
		{
			OBuffer    *buffer = &group->buffers[j];

			LWLockAcquire(&buffer->bufferCtlLock, LW_SHARED);
			if (buffer->dirty &&
				buffer->tag == tag &&
				buffer->blockNum >= firstBufferNumber &&
				buffer->blockNum <= lastBufferNumber)
			{
				write_buffer(desc, buffer);
				buffer->dirty = false;
			}
			LWLockRelease(&buffer->bufferCtlLock);
		}
	}
}

static void
o_buffers_wipe(OBuffersDesc *desc,
			   uint32 tag,
			   int64 firstBufferNumber,
			   int64 lastBufferNumber)
{
	int			i,
				j;

	for (i = 0; i < desc->groupsCount; i++)
	{
		OBuffersGroup *group = &desc->groups[i];

		for (j = 0; j < O_BUFFERS_PER_GROUP; j++)
		{
			OBuffer    *buffer = &group->buffers[j];

			LWLockAcquire(&buffer->bufferCtlLock, LW_EXCLUSIVE);
			if (buffer->dirty &&
				buffer->tag == tag &&
				buffer->blockNum >= firstBufferNumber &&
				buffer->blockNum <= lastBufferNumber)
			{
				buffer->blockNum = -1;
				buffer->dirty = false;
				buffer->tag = 0;
			}
			LWLockRelease(&buffer->bufferCtlLock);
		}
	}
}

void
o_buffers_sync(OBuffersDesc *desc, uint32 tag,
			   int64 fromOffset, int64 toOffset,
			   uint32 wait_event_info)
{
	int64		firstPageNumber,
				lastPageNumber;
	int64		firstFileNumber,
				lastFileNumber,
				fileNumber;

	Assert(OBuffersMaxTagIsValid(tag));

	firstPageNumber = fromOffset / ORIOLEDB_BLCKSZ;
	lastPageNumber = toOffset / ORIOLEDB_BLCKSZ;
	if (toOffset % ORIOLEDB_BLCKSZ == 0)
		lastPageNumber--;

	o_buffers_flush(desc, tag, firstPageNumber, lastPageNumber);

	firstFileNumber = fromOffset / desc->singleFileSize;
	lastFileNumber = toOffset / desc->singleFileSize;
	if (toOffset % desc->singleFileSize == 0)
		lastFileNumber--;

	for (fileNumber = firstFileNumber; fileNumber <= lastFileNumber; fileNumber++)
	{
		open_file(desc, tag, fileNumber);
		FileSync(desc->curFile, wait_event_info);
	}
}

void
o_buffers_unlink_files_range(OBuffersDesc *desc, uint32 tag,
							 int64 firstFileNumber, int64 lastFileNumber)
{
	int64		fileNumber;

	Assert(OBuffersMaxTagIsValid(tag));

	o_buffers_wipe(desc, tag,
				   firstFileNumber * (desc->singleFileSize / ORIOLEDB_BLCKSZ),
				   (lastFileNumber + 1) * (desc->singleFileSize / ORIOLEDB_BLCKSZ) - 1);

	for (fileNumber = firstFileNumber;
		 fileNumber <= lastFileNumber;
		 fileNumber++)
		unlink_file(desc, tag, fileNumber);
}
