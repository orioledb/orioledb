/*-------------------------------------------------------------------------
 *
 * o_buffers.c
 * 		Buffering layer for file access.
 *
 * Copyright (c) 2021-2026, Oriole DB Inc.
 * Copyright (c) 2025-2026, Supabase Inc.
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

/*
 * Open a buffer file.  When create is true, the file is created if it
 * doesn't exist (O_CREAT) and failure is always PANIC.  When create is
 * false, a missing file (ENOENT) returns false instead of panicking.
 */
static bool
open_file(OBuffersDesc *desc, uint32 tag, uint64 fileNum, bool create)
{
	int			flags;

	Assert(OBuffersMaxTagIsValid(tag));

	if (desc->curFile >= 0 &&
		desc->curFileNum == fileNum &&
		desc->curFileTag == tag)
		return true;

	if (desc->curFile >= 0)
		FileClose(desc->curFile);

	pg_snprintf(desc->curFileName, MAXPGPATH,
				desc->filenameTemplate[tag],
				(uint32) (fileNum >> 32),
				(uint32) fileNum);
	flags = O_RDWR | PG_BINARY | (create ? O_CREAT : 0);
	desc->curFile = PathNameOpenFile(desc->curFileName, flags);
	desc->curFileNum = fileNum;
	desc->curFileTag = tag;
	if (desc->curFile < 0)
	{
		if (!create && errno == ENOENT)
			return false;
		ereport(PANIC, (errcode_for_file_access(),
						errmsg("could not open file %s: %m", desc->curFileName)));
	}
	return true;
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

	open_file(desc, tag, blockNum / (desc->singleFileSize / ORIOLEDB_BLCKSZ), true);
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

/*
 * Read a buffer from disk.  When missing_ok is true, a missing file
 * returns false (buffer zeroed) instead of panicking.
 */
static bool
read_buffer(OBuffersDesc *desc, OBuffer *buffer, bool missing_ok)
{
	int			result;

	if (!open_file(desc, buffer->tag,
				   buffer->blockNum / (desc->singleFileSize / ORIOLEDB_BLCKSZ),
				   !missing_ok))
	{
		memset(buffer->data, 0, ORIOLEDB_BLCKSZ);
		return false;
	}

	result = OFileRead(desc->curFile, buffer->data, ORIOLEDB_BLCKSZ,
					   (buffer->blockNum * ORIOLEDB_BLCKSZ) % desc->singleFileSize,
					   WAIT_EVENT_SLRU_READ);

	if (result < 0)
	{
		if (missing_ok)
		{
			memset(buffer->data, 0, ORIOLEDB_BLCKSZ);
			return false;
		}
		ereport(PANIC, (errcode_for_file_access(),
						errmsg("could not read buffer from file %s: %m", desc->curFileName)));
	}

	if (result < ORIOLEDB_BLCKSZ)
		memset(&buffer->data[result], 0, ORIOLEDB_BLCKSZ - result);

	return true;
}

/*
 * Get (or load) a buffer for the given block.  When missing_ok is true and
 * the underlying file does not exist, the buffer is invalidated and NULL is
 * returned (no lock held).  The write flag controls the lock mode for
 * already-cached buffers.
 */
static OBuffer *
get_buffer(OBuffersDesc *desc, uint32 tag, int64 blockNum, bool write,
		   bool missing_ok)
{
	OBuffersGroup *group = &desc->groups[blockNum % desc->groupsCount];
	OBuffer    *buffer = NULL;
	int			i,
				victim = 0;
	uint32		victimUsageCount = 0;
	bool		prevDirty;
	int64		prevBlockNum;
	uint32		prevTag;
	LWLockMode	lockMode = write ? LW_EXCLUSIVE : LW_SHARED;

	/* First check if required buffer is already loaded */
	LWLockAcquire(&group->groupCtlLock, LW_SHARED);
	for (i = 0; i < O_BUFFERS_PER_GROUP; i++)
	{
		buffer = &group->buffers[i];
		if (buffer->blockNum == blockNum &&
			buffer->tag == tag)
		{
			LWLockAcquire(&buffer->bufferCtlLock, lockMode);
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
			LWLockAcquire(&buffer->bufferCtlLock, lockMode);
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

	if (!read_buffer(desc, buffer, missing_ok))
	{
		/* File doesn't exist — invalidate buffer */
		Assert(missing_ok);
		buffer->blockNum = -1;
		buffer->shadowBlockNum = -1;
		LWLockRelease(&buffer->bufferCtlLock);
		return NULL;
	}

	buffer->shadowBlockNum = -1;

	return buffer;
}

/*
 * Read or write a range of data from/to buffers.
 *
 * missing_ok: a missing underlying file returns false instead of panicking
 * (read path returns with buf zeroed).  Otherwise the call always returns
 * true.
 *
 * mark_clean (write only): the data is already durable on disk (the caller
 * is refreshing the cache copy of a page a checkpoint-time flush already
 * pushed out).  The touched buffers are left clean -- no write-back is
 * performed and none must clobber the on-disk image.  Used by the
 * undo/xidmap eviction paths so a stale partial copy a prior eviction left
 * in the cache does not get read back after the written-frontier advances
 * past it.
 */
static bool
o_buffers_rw(OBuffersDesc *desc, Pointer buf,
			 uint32 tag, int64 offset, int64 size,
			 bool write, bool missing_ok, bool mark_clean)
{
	int64		firstBlockNum = offset / ORIOLEDB_BLCKSZ,
				lastBlockNum = (offset + size - 1) / ORIOLEDB_BLCKSZ,
				blockNum;
	Pointer		ptr = buf;

	Assert(OBuffersMaxTagIsValid(tag) && offset >= 0 && size > 0);

	for (blockNum = firstBlockNum; blockNum <= lastBlockNum; blockNum++)
	{
		OBuffer    *buffer = get_buffer(desc, tag, blockNum, write, missing_ok);
		uint32		copySize,
					copyOffset;

		if (!buffer)
		{
			Assert(missing_ok);
			memset(buf, 0, size);
			return false;
		}

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
			buffer->dirty = !mark_clean;
		}
		else
		{
			memcpy(ptr, &buffer->data[copyOffset], copySize);
		}
		ptr += copySize;
		LWLockRelease(&buffer->bufferCtlLock);
	}
	return true;
}

bool
o_buffers_read(OBuffersDesc *desc, Pointer buf, uint32 tag,
			   int64 offset, int64 size, bool if_exists)
{
	return o_buffers_rw(desc, buf, tag, offset, size, false, if_exists, false);
}

bool
o_buffers_write(OBuffersDesc *desc, Pointer buf, uint32 tag,
				int64 offset, int64 size, bool if_exists, bool mark_clean)
{
	return o_buffers_rw(desc, buf, tag, offset, size, true,
						if_exists, mark_clean);
}

/*
 * Write one ORIOLEDB_BLCKSZ-aligned page straight to the on-disk file,
 * bypassing the o_buffers cache.
 *
 * Useful at checkpoint time when the caller knows the same data will
 * stay hot in another in-memory copy (e.g. the xidmap / undo circular
 * buffers, which keep the slot/record contents themselves and answer
 * subsequent reads from there rather than from the o_buffers cache).
 * Going through o_buffers_write() in that case would only pollute the
 * cache with a copy that nobody will ever read.
 *
 * Caller must not call this concurrently for the same OBuffersDesc with
 * other paths that mutate desc->curFile (the process-local open-file
 * cache); the typical pattern is to hold the same write lock that
 * serialises the eviction path.
 *
 * If the page does happen to be resident in the cache -- only the partial
 * boundary pages the eviction path leaves behind ever are -- its copy is
 * refreshed to match what we put on disk and marked clean, so a subsequent
 * read of it does not return now-stale bytes.  Absent pages are deliberately
 * not pulled in: their data stays hot in the caller's ring buffer and a
 * cache copy would only duplicate it.
 */
void
o_buffers_write_page_direct(OBuffersDesc *desc, char *data, uint32 tag,
							int64 offset)
{
	int64		blockNum = offset / ORIOLEDB_BLCKSZ;
	OBuffersGroup *group;
	int			i;

	Assert(OBuffersMaxTagIsValid(tag));
	Assert(offset >= 0 && offset % ORIOLEDB_BLCKSZ == 0);
	Assert(data != NULL);
	Assert(((uintptr_t) data % sizeof(uint64)) == 0);

	group = &desc->groups[blockNum % desc->groupsCount];
	LWLockAcquire(&group->groupCtlLock, LW_SHARED);
	for (i = 0; i < O_BUFFERS_PER_GROUP; i++)
	{
		OBuffer    *buffer = &group->buffers[i];

		if (buffer->blockNum == blockNum && buffer->tag == tag)
		{
			LWLockAcquire(&buffer->bufferCtlLock, LW_EXCLUSIVE);
			memcpy(buffer->data, data, ORIOLEDB_BLCKSZ);
			buffer->dirty = false;
			LWLockRelease(&buffer->bufferCtlLock);
			break;
		}
	}
	LWLockRelease(&group->groupCtlLock);

	write_buffer_data(desc, data, tag, blockNum);
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
		open_file(desc, tag, fileNumber, true);
		FileSync(desc->curFile, wait_event_info);
	}
}

/*
 * Discard blocks [firstBlockNumber, lastBlockNumber]: unlink files fully
 * covered by the range and punch holes over the touched portion of partially
 * covered files at either edge.  A boundary file that does not yet exist
 * (e.g. when callers cleanup ahead of the active write frontier) is silently
 * skipped.
 */
void
o_buffers_unlink_blocks_range(OBuffersDesc *desc, uint32 tag,
							  int64 firstBlockNumber, int64 lastBlockNumber)
{
	int64		blocksPerFile = desc->singleFileSize / ORIOLEDB_BLCKSZ;
	int64		firstFile,
				lastFile,
				fileNumber;

	Assert(OBuffersMaxTagIsValid(tag));

	if (firstBlockNumber > lastBlockNumber)
		return;

	o_buffers_wipe(desc, tag, firstBlockNumber, lastBlockNumber);

	firstFile = firstBlockNumber / blocksPerFile;
	lastFile = lastBlockNumber / blocksPerFile;

	for (fileNumber = firstFile; fileNumber <= lastFile; fileNumber++)
	{
		int64		fileFirstBlock = fileNumber * blocksPerFile;
		int64		fileLastBlock = fileFirstBlock + blocksPerFile - 1;
		int64		from = Max(firstBlockNumber, fileFirstBlock);
		int64		to = Min(lastBlockNumber, fileLastBlock);

		if (from == fileFirstBlock && to == fileLastBlock)
		{
			unlink_file(desc, tag, fileNumber);
		}
		else if (orioledb_use_sparse_files)
		{
			off_t		offset = (off_t) (from - fileFirstBlock) * ORIOLEDB_BLCKSZ;
			off_t		length = (off_t) (to - from + 1) * ORIOLEDB_BLCKSZ;

			if (open_file(desc, tag, fileNumber, false))
				punch_fd_hole(FileGetRawDesc(desc->curFile), offset, length,
							  desc->curFileName);
		}
	}
}

/*
 * Move a reclaim cursor forward, never back.
 *
 * The cursors are only a hint that saves a call from redoing the previous
 * one's work, so a concurrent caller that computed a lower value may lose the
 * race harmlessly -- the worst outcome is one repeated sweep over files that
 * are already gone.
 */
static void
advance_cursor(pg_atomic_uint64 *cursor, int64 fileNumber)
{
	uint64		old = pg_atomic_read_u64(cursor);

	while (old < (uint64) fileNumber)
	{
		if (pg_atomic_compare_exchange_u64(cursor, &old, (uint64) fileNumber))
			break;
	}
}

/*
 * Unlink every file that holds nothing the retain set still needs.
 *
 * The retain set is [chkpRetainStart, chkpRetainEnd) U [transactionRetainStart,
 * +inf), so the dead items form two intervals; a file is reclaimable exactly
 * when it lies wholly inside one of them.  Rounding each interval inwards to
 * whole files gives those files directly, without walking the range.
 *
 * The point of deciding per file against the *current* retain set, rather than
 * per byte range against one caller-supplied range, is that a file straddling
 * an interval edge is simply retried on every later call, until the edge has
 * moved past its end.  The previous scheme unlinked only what a single range
 * covered wholly and never revisited an older range, so the file holding the
 * upper edge of a retired checkpoint retain range stayed on disk for good --
 * one leaked file per checkpoint.
 *
 * Both cursors are low-water marks of "already reclaimed up to here", kept so
 * that a call does not redo the work of the previous one; leaving them at 0
 * only costs one extra sweep, which is how a cluster carrying files leaked by
 * an earlier version reclaims them.
 *
 * itemsPerBlock is the count of caller-defined items in one ORIOLEDB_BLCKSZ
 * block.
 */
void
o_buffers_unlink_dead_files(OBuffersDesc *desc, uint32 tag,
							int64 itemsPerBlock,
							pg_atomic_uint64 *belowCursor,
							pg_atomic_uint64 *gapCursor,
							int64 chkpRetainStart, int64 chkpRetainEnd,
							int64 transactionRetainStart)
{
	int64		blocksPerFile = desc->singleFileSize / ORIOLEDB_BLCKSZ;
	int64		itemsPerFile = blocksPerFile * itemsPerBlock;
	bool		haveChkp = chkpRetainStart < chkpRetainEnd;
	int64		firstFile,
				lastFile;

	/*
	 * Block arithmetic below uses index / itemsPerBlock, which is only
	 * correct when items align to block boundaries.  For straddling items
	 * (e.g. RewindItem) the caller must compute block ranges itself and call
	 * o_buffers_unlink_blocks_range() directly.
	 */
	Assert(ORIOLEDB_BLCKSZ % itemsPerBlock == 0);

	/*
	 * Below the retain set entirely.  The checkpoint range can start above
	 * the live tail, so the lower edge of the retain set is the smaller of
	 * the two -- taking chkpRetainStart alone would reclaim live undo.
	 */
	firstFile = pg_atomic_read_u64(belowCursor);
	lastFile = (haveChkp ? Min(chkpRetainStart, transactionRetainStart)
				: transactionRetainStart) / itemsPerFile;
	if (firstFile < lastFile)
	{
		o_buffers_unlink_blocks_range(desc, tag, firstFile * blocksPerFile,
									  lastFile * blocksPerFile - 1);
		advance_cursor(belowCursor, lastFile);
	}

	/* The gap between the checkpoint range and the live tail. */
	if (haveChkp)
	{
		firstFile = (chkpRetainEnd + itemsPerFile - 1) / itemsPerFile;
		firstFile = Max(firstFile, (int64) pg_atomic_read_u64(gapCursor));
		lastFile = transactionRetainStart / itemsPerFile;
		if (firstFile < lastFile)
		{
			o_buffers_unlink_blocks_range(desc, tag, firstFile * blocksPerFile,
										  lastFile * blocksPerFile - 1);
			advance_cursor(gapCursor, lastFile);
		}
	}
}
