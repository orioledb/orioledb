/*-------------------------------------------------------------------------
 *
 * io.c
 *		Routines for orioledb B-tree disk IO.
 *
 * Copyright (c) 2021-2023, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/btree/io.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <sys/stat.h>
#include <unistd.h>
#include <sys/mman.h>
#include <common/hashfn.h>

#include "orioledb.h"

#include "btree/io.h"
#include "btree/find.h"
#include "btree/merge.h"
#include "btree/page_chunks.h"
#include "btree/undo.h"
#include "checkpoint/checkpoint.h"
#include "catalog/free_extents.h"
#include "recovery/recovery.h"
#include "s3/worker.h"
#include "tableam/descr.h"
#include "tableam/handler.h"
#include "utils/compress.h"
#include "utils/page_pool.h"
#include "utils/seq_buf.h"
#include "utils/stopevent.h"
#include "utils/ucm.h"
#include "workers/bgwriter.h"

#include "access/transam.h"
#include "access/relation.h"
#include "pgstat.h"
#include "storage/bufmgr.h"
#include "utils/memutils.h"
#include "utils/syscache.h"

typedef struct
{
	pg_atomic_uint64 writesStarted;
	pg_atomic_uint64 writesFinished;
	ConditionVariable cv[FLEXIBLE_ARRAY_MEMBER];
} IOShmem;

typedef struct TreeOffset
{
	Oid			datoid;
	Oid			relnode;
	int			segno;
	uint32		chkpNum;
	FileExtent	fileExtent;
	bool		compressed;
} TreeOffset;

typedef struct IOWriteBack
{
	int			extentsNumber;
	int			extentsAllocated;
	TreeOffset *extents;
} IOWriteBack;

static IOWriteBack io_writeback =
{
	0, 0, NULL
};
static LWLockPadded *io_locks;
static IOShmem *ioShmem = NULL;
static int	num_io_lwlocks;
static bool io_in_progress = false;

static bool prepare_non_leaf_page(Page p);
static uint64 get_free_disk_offset(BTreeDescr *desc);
static bool get_free_disk_extent(BTreeDescr *desc, uint32 chkpNum,
								 off_t page_size, FileExtent *extent);
static bool get_free_disk_extent_copy_blkno(BTreeDescr *desc, off_t page_size,
											FileExtent *extent, uint32 checkpoint_number);

static bool write_page_to_disk(BTreeDescr *desc, FileExtent *extent,
							   Pointer page, off_t page_size);
static void write_page(OBTreeFindPageContext *context,
					   OInMemoryBlkno blkno, Page img,
					   uint32 checkpoint_number,
					   bool evict, bool copy_blkno);
static int	tree_offsets_cmp(const void *a, const void *b);
static void writeback_put_extent(IOWriteBack *writeback, BTreeDescr *desc,
								 uint64 downlink);
static void perform_writeback(IOWriteBack *writeback);

PG_FUNCTION_INFO_V1(orioledb_evict_pages);
PG_FUNCTION_INFO_V1(orioledb_write_pages);

Size
btree_io_shmem_needs(void)
{
	return CACHELINEALIGN(offsetof(IOShmem, cv) +
						  sizeof(ConditionVariable) * max_procs);
}

void
btree_io_shmem_init(Pointer buf, bool found)
{
	Pointer		ptr = buf;

	ioShmem = (IOShmem *) ptr;
	if (!found)
	{
		int			i;

		pg_atomic_init_u64(&ioShmem->writesStarted, 0);
		pg_atomic_init_u64(&ioShmem->writesFinished, 0);

		for (i = 0; i < max_procs; i++)
			ConditionVariableInit(&ioShmem->cv[i]);
	}
}

static void
io_start(void)
{
	uint64		startNum;
	bool		slept = false;

	if (max_io_concurrency == 0)
		return;

	startNum = pg_atomic_add_fetch_u64(&ioShmem->writesStarted, 1);
	io_in_progress = true;
	while (startNum > pg_atomic_read_u64(&ioShmem->writesFinished) + max_io_concurrency)
	{
		ConditionVariableSleep(&ioShmem->cv[startNum % max_procs], WAIT_EVENT_PG_SLEEP);
		slept = true;
	}
	if (slept)
		ConditionVariableCancelSleep();
}

static void
io_finish(void)
{
	uint64		finishNum;

	if (max_io_concurrency == 0)
		return;

	finishNum = pg_atomic_add_fetch_u64(&ioShmem->writesFinished, 1);
	io_in_progress = false;
	ConditionVariableBroadcast(&ioShmem->cv[(finishNum + max_io_concurrency) % max_procs]);
}

int
OFileRead(File file, char *buffer, int amount, off_t offset,
		  uint32 wait_event_info)
{
	int			result;

	io_start();
	result = FileRead(file, buffer, amount, offset, wait_event_info);
	io_finish();
	return result;
}

int
OFileWrite(File file, char *buffer, int amount, off_t offset,
		   uint32 wait_event_info)
{
	int			result;

	io_start();
	result = FileWrite(file, buffer, amount, offset, wait_event_info);
	io_finish();
	return result;
}

typedef struct
{
	uint32		checkpointNumber;
	uint32		segmentNumber;
} FileHashKey;

typedef struct
{
	FileHashKey key;
	File		file;
	char		status;			/* for simplehash use */
} FileHashElement;

#define SH_PREFIX s3Files
#define SH_ELEMENT_TYPE FileHashElement
#define SH_KEY_TYPE FileHashKey
#define SH_KEY key
#define SH_HASH_KEY(tb, key) hash_any((unsigned char *) &key, sizeof(FileHashKey))
#define SH_EQUAL(tb, a, b) memcmp(&a, &b, sizeof(FileHashKey)) == 0
#define SH_SCOPE static inline
#define SH_GET_HASH(tb, a) a->hash
#define SH_DEFINE
#define SH_DECLARE
#include "lib/simplehash.h"

char *
btree_filename(Oid datoid, Oid relnode, int segno, uint32 chkpNum)
{
	o_check_init_db_dir(datoid);

	if (orioledb_s3_mode)
	{
		if (segno == 0)
			return psprintf(ORIOLEDB_DATA_DIR "/%u/%u-%u",
							datoid,
							relnode,
							chkpNum);
		else
			return psprintf(ORIOLEDB_DATA_DIR "/%u/%u.%u-%u",
							datoid,
							relnode,
							segno,
							chkpNum);
	}
	else
	{
		if (segno == 0)
			return psprintf(ORIOLEDB_DATA_DIR "/%u/%u",
							datoid,
							relnode);
		else
			return psprintf(ORIOLEDB_DATA_DIR "/%u/%u.%u",
							datoid,
							relnode,
							segno);
	}
}

char *
btree_smgr_filename(BTreeDescr *desc, off_t offset, uint32 chkpNum)
{
	int			segno = offset / ORIOLEDB_SEGMENT_SIZE;

	o_check_init_db_dir(desc->oids.datoid);

	return btree_filename(desc->oids.datoid,
						  desc->oids.relnode,
						  segno,
						  chkpNum);
}

static File
btree_open_smgr_file(BTreeDescr *desc, uint32 num, uint32 chkpNum)
{
	if (orioledb_s3_mode)
	{
		FileHashElement *hashElem;
		FileHashKey key;
		bool		found;
		char	   *filename;

		key.checkpointNumber = chkpNum;
		key.segmentNumber = num;
		hashElem = s3Files_insert(desc->smgr.hash, key, &found);
		if (found)
			return hashElem->file;

		filename = btree_smgr_filename(desc,
									   (off_t) num * ORIOLEDB_SEGMENT_SIZE,
									   chkpNum);
		hashElem->file = PathNameOpenFile(filename, O_RDWR | O_CREAT | PG_BINARY);
		if (hashElem->file <= 0)
			ereport(FATAL,
					(errcode_for_file_access(),
					 errmsg("could not open data file %s", filename)));
		pfree(filename);
		return hashElem->file;
	}
	else
	{
		char	   *filename;

		if (num >= desc->smgr.array.filesAllocated)
		{
			int			i = desc->smgr.array.filesAllocated;

			while (num >= desc->smgr.array.filesAllocated)
				desc->smgr.array.filesAllocated *= 2;
			desc->smgr.array.files = (File *) repalloc(desc->smgr.array.files,
													   sizeof(File) * desc->smgr.array.filesAllocated);
			for (; i < desc->smgr.array.filesAllocated; i++)
				desc->smgr.array.files[i] = -1;
		}

		if (desc->smgr.array.files[num] >= 0)
			return desc->smgr.array.files[num];

		filename = btree_smgr_filename(desc,
									   (off_t) num * ORIOLEDB_SEGMENT_SIZE,
									   chkpNum);
		desc->smgr.array.files[num] = PathNameOpenFile(filename, O_RDWR | O_CREAT | PG_BINARY);

		if (desc->smgr.array.files[num] <= 0)
			ereport(FATAL,
					(errcode_for_file_access(),
					 errmsg("could not open data file %s", filename)));
		pfree(filename);
		return desc->smgr.array.files[num];
	}
}

void
btree_init_smgr(BTreeDescr *descr)
{
	if (orioledb_s3_mode)
	{
		descr->smgr.hash = NULL;
	}
	else
	{
		descr->smgr.array.files = NULL;
		descr->smgr.array.filesAllocated = 0;
	}
}

void
btree_open_smgr(BTreeDescr *descr)
{
	if (orioledb_s3_mode)
	{
		descr->smgr.hash = s3Files_create(TopMemoryContext, 16, NULL);
	}
	else
	{
		int			i;

		if (descr->smgr.array.files)
			return;

		descr->smgr.array.filesAllocated = 16;
		descr->smgr.array.files = (File *) MemoryContextAlloc(TopMemoryContext,
															  sizeof(File) * descr->smgr.array.filesAllocated);
		for (i = 0; i < descr->smgr.array.filesAllocated; i++)
			descr->smgr.array.files[i] = -1;
		(void) btree_open_smgr_file(descr, 0, 0);
	}
}

void
btree_close_smgr(BTreeDescr *descr)
{
	int			i;

	if (orioledb_s3_mode)
	{
		if (descr->smgr.hash)
			s3Files_destroy(descr->smgr.hash);
	}
	else if (descr->smgr.array.files)
	{
		for (i = 0; i < descr->smgr.array.filesAllocated; i++)
		{
			if (descr->smgr.array.files[i] >= 0)
				FileClose(descr->smgr.array.files[i]);
		}
		pfree(descr->smgr.array.files);
	}
	descr->smgr.array.filesAllocated = 0;
	descr->smgr.array.files = NULL;
}

static void
btree_s3_flush(BTreeDescr *desc, uint32 chkpNum)
{
	int			i;
	BTreeMetaPage *meta = BTREE_GET_META(desc);

	for (i = 0; i < MAX_NUM_DIRTY_PARTS; i++)
	{
		S3TaskLocation location;
		int32		segNum,
					partNum;

		segNum = meta->partsInfo[chkpNum % 2].dirtyParts[i].segNum;
		partNum = meta->partsInfo[chkpNum % 2].dirtyParts[i].partNum;
		if (segNum >= 0 && partNum >= 0)
		{
			location = s3_schedule_file_part_write(chkpNum,
												   desc->oids.datoid,
												   desc->oids.relnode,
												   segNum,
												   partNum);
			meta->partsInfo[chkpNum % 2].writeMaxLocation =
				Max(meta->partsInfo[chkpNum % 2].writeMaxLocation, location);
		}
		meta->partsInfo[chkpNum % 2].dirtyParts[i].segNum = -1;
		meta->partsInfo[chkpNum % 2].dirtyParts[i].partNum = -1;
	}
}

static void
btree_smgr_schedule_s3_write(BTreeDescr *desc, uint32 chkpNum,
							 int32 segNum, int32 partNum)
{
	int			i;
	int32		curSegNum,
				curPartNum,
				tmpSegNum,
				tmpPartNum;
	BTreeMetaPage *meta = BTREE_GET_META(desc);

	curSegNum = segNum;
	curPartNum = partNum;
	for (i = 0; i < MAX_NUM_DIRTY_PARTS; i++)
	{
		tmpSegNum = meta->partsInfo[chkpNum % 2].dirtyParts[i].segNum;
		tmpPartNum = meta->partsInfo[chkpNum % 2].dirtyParts[i].partNum;
		meta->partsInfo[chkpNum % 2].dirtyParts[i].segNum = curSegNum;
		meta->partsInfo[chkpNum % 2].dirtyParts[i].partNum = curPartNum;
		curSegNum = tmpSegNum;
		curPartNum = tmpPartNum;

		if ((curSegNum == segNum && curPartNum == partNum) || curSegNum < 0)
			break;

		if (i == MAX_NUM_DIRTY_PARTS - 1)
		{
			S3TaskLocation location;

			location = s3_schedule_file_part_write(chkpNum,
												   desc->oids.datoid,
												   desc->oids.relnode,
												   curSegNum,
												   curPartNum);
			meta->partsInfo[chkpNum % 2].writeMaxLocation =
				Max(meta->partsInfo[chkpNum % 2].writeMaxLocation, location);
		}
	}
}

static int
btree_smgr_write(BTreeDescr *desc, char *buffer, uint32 chkpNum,
				 int amount, off_t offset)
{
	int			result = 0;

	if (orioledb_s3_mode)
	{
		btree_smgr_schedule_s3_write(desc,
									 chkpNum,
									 offset / ORIOLEDB_SEGMENT_SIZE,
									 (offset % ORIOLEDB_SEGMENT_SIZE) / ORIOLEDB_S3_PART_SIZE);
		if (offset / ORIOLEDB_S3_PART_SIZE != (offset + amount - 1) / ORIOLEDB_S3_PART_SIZE)
			btree_smgr_schedule_s3_write(desc,
										 chkpNum,
										 (offset + amount - 1) / ORIOLEDB_SEGMENT_SIZE,
										 ((offset + amount - 1) % ORIOLEDB_SEGMENT_SIZE) / ORIOLEDB_S3_PART_SIZE);
	}

	if (use_mmap)
	{
		Assert(offset + amount <= device_length);
		memcpy(mmap_data + offset, buffer, amount);
		return amount;
	}
	else if (use_device)
	{
		Assert(offset + amount <= device_length);
		pgstat_report_wait_start(WAIT_EVENT_DATA_FILE_WRITE);
		result = pg_pwrite(device_fd, buffer, amount, offset);
		pgstat_report_wait_end();
		return result;
	}

	while (amount > 0)
	{
		int			segno = offset / ORIOLEDB_SEGMENT_SIZE;
		File		file;

		file = btree_open_smgr_file(desc, segno, chkpNum);
		if ((offset + amount) / ORIOLEDB_SEGMENT_SIZE == segno)
		{
			result += OFileWrite(file, buffer, amount,
								 offset % ORIOLEDB_SEGMENT_SIZE,
								 WAIT_EVENT_DATA_FILE_WRITE);
			break;
		}
		else
		{
			int			stepAmount = ORIOLEDB_SEGMENT_SIZE - offset % ORIOLEDB_SEGMENT_SIZE;

			Assert(amount >= stepAmount);
			result += OFileWrite(file, buffer, stepAmount,
								 offset % ORIOLEDB_SEGMENT_SIZE,
								 WAIT_EVENT_DATA_FILE_WRITE);
			buffer += stepAmount;
			offset += stepAmount;
			amount -= stepAmount;
		}
	}
	return result;
}

int
btree_smgr_read(BTreeDescr *desc, char *buffer, uint32 chkpNum,
				int amount, off_t offset)
{
	int			result = 0;

	if (use_mmap)
	{
		Assert(offset + amount <= device_length);
		memcpy(buffer, mmap_data + offset, amount);
		return amount;
	}
	else if (use_device)
	{
		Assert(offset + amount <= device_length);
		pgstat_report_wait_start(WAIT_EVENT_DATA_FILE_READ);
		result = pg_pread(device_fd, buffer, amount, offset);
		pgstat_report_wait_end();
		return result;
	}

	while (amount > 0)
	{
		int			segno = offset / ORIOLEDB_SEGMENT_SIZE;
		File		file;

		file = btree_open_smgr_file(desc, segno, chkpNum);
		if ((offset + amount) / ORIOLEDB_SEGMENT_SIZE == segno)
		{
			result += OFileRead(file, buffer, amount,
								offset % ORIOLEDB_SEGMENT_SIZE,
								WAIT_EVENT_DATA_FILE_READ);
			break;
		}
		else
		{
			int			stepAmount = ORIOLEDB_SEGMENT_SIZE - offset % ORIOLEDB_SEGMENT_SIZE;

			Assert(amount >= stepAmount);
			result += OFileRead(file, buffer, stepAmount,
								offset % ORIOLEDB_SEGMENT_SIZE,
								WAIT_EVENT_DATA_FILE_READ);
			buffer += stepAmount;
			offset += stepAmount;
			amount -= stepAmount;
		}
	}

	return result;
}

void
btree_smgr_writeback(BTreeDescr *desc, uint32 chkpNum,
					 off_t offset, int amount)
{
	if (use_mmap)
	{
		Assert(offset + amount <= device_length);
		msync(mmap_data + offset, amount, MS_ASYNC);
		return;
	}
	else if (use_device)
	{
		return;
	}

	while (amount > 0)
	{
		int			segno = offset / ORIOLEDB_SEGMENT_SIZE;
		File		file;

		file = btree_open_smgr_file(desc, segno, chkpNum);
		if ((offset + amount) / ORIOLEDB_SEGMENT_SIZE == segno)
		{
			FileWriteback(file, offset % ORIOLEDB_SEGMENT_SIZE,
						  amount, WAIT_EVENT_DATA_FILE_FLUSH);
			break;
		}
		else
		{
			int			stepAmount = ORIOLEDB_SEGMENT_SIZE - offset % ORIOLEDB_SEGMENT_SIZE;

			Assert(amount >= stepAmount);
			FileWriteback(file, offset % ORIOLEDB_SEGMENT_SIZE,
						  stepAmount, WAIT_EVENT_DATA_FILE_FLUSH);
			offset += stepAmount;
			amount -= stepAmount;
		}
	}
}

void
btree_smgr_sync(BTreeDescr *desc, uint32 chkpNum, off_t length)
{
	int			num;

	if (orioledb_s3_mode)
		btree_s3_flush(desc, chkpNum);

	if (use_mmap || use_device)
		return;

	for (num = 0; num < length / ORIOLEDB_SEGMENT_SIZE; num++)
	{
		File		file;

		file = btree_open_smgr_file(desc, num, chkpNum);
		FileSync(file, WAIT_EVENT_DATA_FILE_SYNC);
	}
}

void
btree_io_error_cleanup(void)
{
	if (io_in_progress)
		io_finish();
}

void
request_btree_io_lwlocks(void)
{
	num_io_lwlocks = max_procs * 4;
	RequestNamedLWLockTranche("orioledb_btree_io", num_io_lwlocks);
}

void
init_btree_io_lwlocks(void)
{
	io_locks = GetNamedLWLockTranche("orioledb_btree_io");
}

/*
 * Assign number of IO operation to particular (blkno; offnum) pair.
 */
int
assign_io_num(OInMemoryBlkno blkno, OffsetNumber offnum)
{
	int			locknum;
	int			i;
	pg_crc32c	crc;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, &blkno, sizeof(blkno));
	COMP_CRC32C(crc, &offnum, sizeof(offnum));
	FIN_CRC32C(crc);

	locknum = crc % num_io_lwlocks;

	for (i = 0; i < num_io_lwlocks; i++)
	{
		if (LWLockConditionalAcquire(&io_locks[locknum].lock, LW_EXCLUSIVE))
			return locknum;
		locknum = (locknum + 1) % num_io_lwlocks;
	}

	LWLockAcquire(&io_locks[locknum].lock, LW_EXCLUSIVE);
	return locknum;
}

/*
 * Wait until particular IO operation is completed.
 */
void
wait_for_io_completion(int ionum)
{
	LWLockAcquire(&io_locks[ionum].lock, LW_SHARED);
	LWLockRelease(&io_locks[ionum].lock);
}

/*
 * Report given IO operation to be finished.
 */
void
unlock_io(int ionum)
{
	LWLockRelease(&io_locks[ionum].lock);
}

/*
 * Get next disk free offset for uncompressed on disk B-tree.
 * Returns InvalidFileExtentOff if fails.
 */
static uint64
get_free_disk_offset(BTreeDescr *desc)
{
	BTreeMetaPage *metaPage = BTREE_GET_META(desc);
	LWLock	   *metaLock = &metaPage->metaLock;
	uint64		result,
				numFreeBlocks;
	uint32		free_buf_num;
	bool		gotBlock;

	Assert(!orioledb_s3_mode);

	/*
	 * Switch to the next sequential buffer with free blocks numbers in
	 * needed.
	 */
	numFreeBlocks = pg_atomic_read_u64(&metaPage->numFreeBlocks);
	free_buf_num = metaPage->freeBuf.tag.num;
	while (numFreeBlocks == 0 &&
		   can_use_checkpoint_extents(desc, free_buf_num + 1))
	{
		SeqBufTag	tag = {0},
					old_tag = desc->freeBuf.shared->tag;
		SeqBufReplaceResult replaceResult;

		tag.datoid = desc->oids.datoid;
		tag.relnode = desc->oids.relnode;
		tag.num = free_buf_num + 1;
		tag.type = 't';

		LWLockAcquire(metaLock, LW_EXCLUSIVE);
		replaceResult = seq_buf_try_replace(&desc->freeBuf,
											&tag,
											&metaPage->numFreeBlocks,
											use_device ? sizeof(FileExtent) : sizeof(uint32));
		if (replaceResult == SeqBufReplaceSuccess)
		{
			seq_buf_remove_file(&old_tag);
		}
		LWLockRelease(metaLock);
		if (replaceResult == SeqBufReplaceError)
		{
			return InvalidFileExtentOff;
		}
		/* SeqBufReplaceAlready requires no action, just retry if needed */

		numFreeBlocks = pg_atomic_read_u64(&metaPage->numFreeBlocks);
		free_buf_num = metaPage->freeBuf.tag.num;
	}

	/*
	 * Try to get free block number from the buffer.  If not success, then
	 * extend the file.
	 */
	LWLockAcquire(metaLock, LW_SHARED);
	gotBlock = false;
	while (numFreeBlocks > 0)
	{
		if (pg_atomic_compare_exchange_u64(&metaPage->numFreeBlocks,
										   &numFreeBlocks,
										   numFreeBlocks - 1))
		{
			gotBlock = true;
			break;
		}
	}

	if (gotBlock)
	{

		if (use_device)
		{
			FileExtent	extent;

			if (seq_buf_read_file_extent(&desc->freeBuf, &extent))
				result = extent.off;
			else
				result = InvalidFileExtentOff;
		}
		else
		{
			uint32		offset;

			if (seq_buf_read_u32(&desc->freeBuf, &offset))
				result = offset;
			else
				result = InvalidFileExtentOff;
		}
	}
	else
	{
		if (use_device)
			result = orioledb_device_alloc(desc, ORIOLEDB_BLCKSZ) / ORIOLEDB_COMP_BLCKSZ;
		else
			result = pg_atomic_fetch_add_u64(&metaPage->datafileLength[0], 1);
	}
	LWLockRelease(metaLock);
	return result;
}

/*
 * Fills free file extent for B-tree.
 *
 * FileExtentIsValid(extent) == false if fails.
 */
static bool
get_free_disk_extent(BTreeDescr *desc, uint32 chkpNum,
					 off_t page_size, FileExtent *extent)
{
	if (orioledb_s3_mode)
	{
		int			len = OCompressIsValid(desc->compress) ? FileExtentLen(page_size) : 1;
		BTreeMetaPage *metaPage = BTREE_GET_META(desc);

		extent->off = pg_atomic_fetch_add_u64(&metaPage->datafileLength[chkpNum % 2], len);
		extent->len = len;

		extent->off |= (uint64) chkpNum << S3_CHKP_NUM_SHIFT;

		return FileExtentIsValid(*extent);
	}


	if (!OCompressIsValid(desc->compress))
	{
		Assert(page_size == ORIOLEDB_BLCKSZ);

		extent->off = get_free_disk_offset(desc);
		extent->len = 1;
	}
	else
	{
		/* Try to add free extents if we didn't manage to do after checkpoint */
		add_free_extents_from_tmp(desc, remove_old_checkpoint_files);
		*extent = get_extent(desc, FileExtentLen(page_size));
	}

	return FileExtentIsValid(*extent);
}

/*
 * Fills free file extent for B-tree under copy blkno lock.
 *
 * FileExtentIsValid(extent) == false if fails.
 */
static bool
get_free_disk_extent_copy_blkno(BTreeDescr *desc, off_t page_size,
								FileExtent *extent, uint32 checkpoint_number)
{
	BTreeMetaPage *metaPage = BTREE_GET_META(desc);

	LWLockAcquire(&metaPage->copyBlknoLock, LW_SHARED);

	if (!get_free_disk_extent(desc, checkpoint_number, page_size, extent))
	{
		LWLockRelease(&metaPage->copyBlknoLock);
		return false;
	}

	if (desc->storageType == BTreeStoragePersistence &&
		checkpoint_state->treeType == desc->type &&
		checkpoint_state->datoid == desc->oids.datoid &&
		checkpoint_state->relnode == desc->oids.relnode &&
		checkpoint_state->curKeyType != CurKeyFinished)
	{
		/*
		 * We're writing to the next checkpoint, while current checkpoint is
		 * concurrently taking.  So, indicate this page is free in the
		 * checkpoint currently taking.  We have to take a lock in order to be
		 * sure that checkpoint map file will be finishing concurrently.
		 * Otherwise we might loose this block number.
		 */
		int			prev_chkp_index = (checkpoint_number - 1) % 2;
		bool		success;

		if (OCompressIsValid(desc->compress) || use_device)
		{
			success = seq_buf_write_file_extent(&desc->nextChkp[prev_chkp_index], *extent);
		}
		else
		{
			uint32		offset = extent->off;

			Assert(offset < UINT32_MAX);
			success = seq_buf_write_u32(&desc->nextChkp[prev_chkp_index], offset);
		}

		if (!success)
		{
			LWLockRelease(&metaPage->copyBlknoLock);
			return false;
		}
	}

	LWLockRelease(&metaPage->copyBlknoLock);

	return FileExtentIsValid(*extent);
}

/*
 * Reads a page from disk to the img from a valid downlink. It's fills an empty
 * array of offsets for the page.
 */
bool
read_page_from_disk(BTreeDescr *desc, Pointer img, uint64 downlink,
					FileExtent *extent)
{
	off_t		byte_offset,
				read_size;
	uint64		offset = DOWNLINK_GET_DISK_OFF(downlink);
	uint32		chkpNum = 0;
	uint16		len = DOWNLINK_GET_DISK_LEN(downlink);
	bool		err = false;

	Assert(FileExtentOffIsValid(offset));
	Assert(FileExtentLenIsValid(len));

	if (!OCompressIsValid(desc->compress))
	{
		/* easy case, read page from uncompressed index */
		Assert(len == 1);
		extent->off = offset;
		extent->len = 1;

		if (orioledb_s3_mode)
		{
			chkpNum = S3_GET_CHKP_NUM(offset);
			offset &= S3_OFFSET_MASK;
		}

		if (use_device)
			byte_offset = (off_t) offset * (off_t) ORIOLEDB_COMP_BLCKSZ;
		else
			byte_offset = (off_t) offset * (off_t) ORIOLEDB_BLCKSZ;
		read_size = ORIOLEDB_BLCKSZ;

		err = btree_smgr_read(desc, img, chkpNum, read_size, byte_offset) != read_size;
	}
	else
	{
		char		buf[ORIOLEDB_BLCKSZ];
		bool		compressed = len != (ORIOLEDB_BLCKSZ / ORIOLEDB_COMP_BLCKSZ);
		Pointer		read_buf = compressed ? buf : img;

		extent->off = offset;
		extent->len = len;

		if (orioledb_s3_mode)
		{
			chkpNum = S3_GET_CHKP_NUM(offset);
			offset &= S3_OFFSET_MASK;
		}

		byte_offset = (off_t) offset * (off_t) ORIOLEDB_COMP_BLCKSZ;
		read_size = len * ORIOLEDB_COMP_BLCKSZ;

		err = btree_smgr_read(desc, read_buf, chkpNum, read_size, byte_offset) != read_size;;

		if (!err && compressed)
		{
			OCompressHeader header;

			memcpy(&header, buf, sizeof(OCompressHeader));
			o_decompress_page(buf + sizeof(OCompressHeader), header, img);
		}
	}

	return !err;
}

/*
 * Writes a page to the disk. An array of file offsets must be valid.
 */
static bool
write_page_to_disk(BTreeDescr *desc, FileExtent *extent,
				   Pointer page, off_t page_size)
{

	off_t		byte_offset,
				write_size;
	bool		err = false;
	uint32		chkpNum = 0;

	Assert(FileExtentOffIsValid(extent->off));
	if (!OCompressIsValid(desc->compress))
	{
		/* easy case, write page to uncompressed index */
		Assert(extent->len == 1);
		Assert(page_size == ORIOLEDB_BLCKSZ);

		byte_offset = (off_t) extent->off;

		if (orioledb_s3_mode)
		{
			chkpNum = S3_GET_CHKP_NUM(byte_offset);
			byte_offset &= S3_OFFSET_MASK;
		}

		if (use_device)
			byte_offset *= (off_t) ORIOLEDB_COMP_BLCKSZ;
		else
			byte_offset *= (off_t) ORIOLEDB_BLCKSZ;
		write_size = ORIOLEDB_BLCKSZ;

		err = btree_smgr_write(desc, page, chkpNum, write_size, byte_offset) != write_size;
	}
	else
	{
		byte_offset = (off_t) extent->off;
		if (orioledb_s3_mode)
		{
			chkpNum = S3_GET_CHKP_NUM(byte_offset);
			byte_offset &= S3_OFFSET_MASK;
		}
		byte_offset *= (off_t) ORIOLEDB_COMP_BLCKSZ;

		if (page_size != ORIOLEDB_BLCKSZ)
		{
			/* we need to write header first */
			OCompressHeader header = page_size;

			/*
			 * overflow protection
			 */
			Assert(page_size < ORIOLEDB_BLCKSZ);
			Assert(sizeof(OCompressHeader) == sizeof(uint16));
			Assert(ORIOLEDB_BLCKSZ < UINT16_MAX);

			write_size = sizeof(OCompressHeader);
			err = btree_smgr_write(desc, (char *) &header, chkpNum, write_size, byte_offset) != write_size;
			byte_offset += write_size;

			if (err)
				return false;

			write_size = extent->len * ORIOLEDB_COMP_BLCKSZ - sizeof(OCompressHeader);
		}
		else
		{
			write_size = ORIOLEDB_BLCKSZ;
		}

		/* write data */
		err = btree_smgr_write(desc, page, chkpNum, write_size, byte_offset) != write_size;
	}

	return !err;
}

/*
 * Load the page where context is pointing from disk to memory, assuming parent
 * page is locked.
 */
void
load_page(OBTreeFindPageContext *context)
{
	OrioleDBPageDesc *parent_page_desc,
			   *page_desc;
	BTreeDescr *desc = context->desc;
	OInMemoryBlkno parent_blkno;
	Page		parent_page;
	BTreePageItemLocator *parent_loc;
	CommitSeqNo csn;
	uint64		downlink;
	int			context_index,
				ionum;
	uint32		parent_change_count;
	BTreeNonLeafTuphdr *int_hdr;
	OInMemoryBlkno blkno;
	Page		page;
	char		buf[ORIOLEDB_BLCKSZ];
	bool		was_modify;
	bool		was_downlink_location;
	bool		was_fetch = false;
	bool		was_image = false;
	bool		was_keep_lokey = false;
	uint32		chkpNum = 0;

	context_index = context->index;
	parent_blkno = context->items[context_index].blkno;
	parent_loc = &context->items[context_index].locator;
	parent_change_count = context->items[context_index].pageChangeCount;
	parent_page = O_GET_IN_MEMORY_PAGE(parent_blkno);

	ionum = assign_io_num(parent_blkno, BTREE_PAGE_LOCATOR_GET_OFFSET(parent_page, parent_loc));

	/* Modify parent downlink: indicate that IO is in-progress */
	page_block_reads(parent_blkno);
	int_hdr = (BTreeNonLeafTuphdr *) BTREE_PAGE_LOCATOR_GET_ITEM(parent_page, parent_loc);
	Assert(DOWNLINK_IS_ON_DISK(int_hdr->downlink));

	downlink = int_hdr->downlink;

	int_hdr->downlink = MAKE_IO_DOWNLINK(ionum);
	Assert(PAGE_GET_N_ONDISK(parent_page) > 0);
	PAGE_DEC_N_ONDISK(parent_page);
	unlock_page(parent_blkno);

	/* Prepare new page metaPage-data */
	ppool_reserve_pages(desc->ppool, PPOOL_RESERVE_FIND, 1);
	blkno = ppool_get_page(desc->ppool, PPOOL_RESERVE_FIND);
	lock_page(blkno);
	page_block_reads(blkno);

	Assert(OInMemoryBlknoIsValid(blkno));
	page = O_GET_IN_MEMORY_PAGE(blkno);
	parent_page_desc = O_GET_IN_MEMORY_PAGEDESC(parent_blkno);
	page_desc = O_GET_IN_MEMORY_PAGEDESC(blkno);

	page_desc->flags = 0;

	/* Read page data and put it to the page */
	if (!read_page_from_disk(desc, buf, downlink, &page_desc->fileExtent))
	{
		int_hdr->downlink = downlink;
		PAGE_INC_N_ONDISK(parent_page);
		unlock_io(ionum);
		if (orioledb_s3_mode)
			chkpNum = S3_GET_CHKP_NUM(page_desc->fileExtent.off);

		ereport(ERROR, (errcode_for_file_access(),
						errmsg("could not read page with file offset " UINT64_FORMAT " from %s",
							   DOWNLINK_GET_DISK_OFF(downlink),
							   btree_smgr_filename(desc, DOWNLINK_GET_DISK_OFF(downlink), chkpNum))));
	}

	put_page_image(blkno, buf);
	page_change_usage_count(&desc->ppool->ucm, blkno,
							(pg_atomic_read_u32(desc->ppool->ucm.epoch) + 2) % UCM_USAGE_LEVELS);
	page_desc->type = parent_page_desc->type;
	page_desc->oids = parent_page_desc->oids;

	Assert(O_PAGE_IS(page, LEAF) ||
		   (PAGE_GET_N_ONDISK(page) == BTREE_PAGE_ITEMS_COUNT(page)));

	if (orioledb_s3_mode && !O_PAGE_IS(page, LEAF))
	{
		BTreePageItemLocator loc;

		/*
		 * In S3 mode schedule load of all the page children for faster warmup.
		 */
		BTREE_PAGE_FOREACH_ITEMS(page, &loc)
		{
			BTreeNonLeafTuphdr *tupHdr;

			tupHdr =  (BTreeNonLeafTuphdr *) BTREE_PAGE_LOCATOR_GET_ITEM(page, &loc);
			s3_schedule_downlink_load(desc->oids.datoid,
									  desc->oids.relnode,
									  tupHdr->downlink);
		}
	}

	unlock_page(blkno);

	EA_LOAD_INC(blkno);

	if (STOPEVENTS_ENABLED())
	{
		Jsonb	   *params;

		params = btree_page_stopevent_params(desc, page);
		STOPEVENT(STOPEVENT_LOAD_PAGE_REFIND, params);
	}

	/* re-find parent page (it might be changed due to concurrent operations) */
	csn = context->csn;
	was_modify = BTREE_PAGE_FIND_IS(context, MODIFY);
	was_image = BTREE_PAGE_FIND_IS(context, IMAGE);
	BTREE_PAGE_FIND_UNSET(context, IMAGE);
	if (!was_modify)
	{
		was_fetch = BTREE_PAGE_FIND_IS(context, FETCH);
		Assert(was_fetch || was_image);
		BTREE_PAGE_FIND_UNSET(context, FETCH);
		BTREE_PAGE_FIND_SET(context, MODIFY);
	}
	was_keep_lokey = BTREE_PAGE_FIND_IS(context, KEEP_LOKEY);
	if (was_keep_lokey)
		BTREE_PAGE_FIND_UNSET(context, KEEP_LOKEY);
	was_downlink_location = BTREE_PAGE_FIND_IS(context, DOWNLINK_LOCATION);
	if (!was_downlink_location)
		BTREE_PAGE_FIND_SET(context, DOWNLINK_LOCATION);
	context->csn = COMMITSEQNO_INPROGRESS;

	if (O_PAGE_IS(page, RIGHTMOST))
	{
		refind_page(context, NULL, BTreeKeyRightmost, PAGE_GET_LEVEL(page) + 1,
					parent_blkno, parent_change_count);
	}
	else
	{
		OTuple		hikey;

		BTREE_PAGE_GET_HIKEY(hikey, page);
		refind_page(context, &hikey, BTreeKeyPageHiKey,
					PAGE_GET_LEVEL(page) + 1, parent_blkno, parent_change_count);
	}

	/* restore context state */
	context->csn = csn;
	if (!was_modify)
	{
		if (was_fetch)
			BTREE_PAGE_FIND_SET(context, FETCH);
		BTREE_PAGE_FIND_UNSET(context, MODIFY);
	}
	if (was_image)
		BTREE_PAGE_FIND_SET(context, IMAGE);
	if (was_keep_lokey)
		BTREE_PAGE_FIND_SET(context, KEEP_LOKEY);
	if (!was_downlink_location)
		BTREE_PAGE_FIND_UNSET(context, DOWNLINK_LOCATION);

	context_index = context->index;
	parent_blkno = context->items[context_index].blkno;
	parent_loc = &context->items[context_index].locator;
	parent_change_count = context->items[context_index].pageChangeCount;

	/* Replace parent downlink with orioledb downlink */
	page_block_reads(parent_blkno);
	parent_page = O_GET_IN_MEMORY_PAGE(parent_blkno);
	int_hdr = (BTreeNonLeafTuphdr *) BTREE_PAGE_LOCATOR_GET_ITEM(parent_page, parent_loc);
	Assert(int_hdr->downlink == MAKE_IO_DOWNLINK(ionum));
	int_hdr->downlink = MAKE_IN_MEMORY_DOWNLINK(blkno, O_PAGE_HEADER(page)->pageChangeCount);

	unlock_io(ionum);
}

/*
 * Returns pointer to writable image. It compresses page if needed.
 */
static inline Pointer
get_write_img(BTreeDescr *desc, Page page, size_t *size)
{
	Pointer		result;

	if (OCompressIsValid(desc->compress))
	{
		result = o_compress_page(page, size, desc->compress);
		if (*size > (ORIOLEDB_BLCKSZ - ORIOLEDB_COMP_BLCKSZ - sizeof(OCompressHeader)))
		{
			/*
			 * No sense to write compressed page
			 */
			result = page;
			*size = ORIOLEDB_BLCKSZ;
		}
	}
	else
	{
		result = page;
		*size = ORIOLEDB_BLCKSZ;
	}
	return result;
}

#ifdef USE_ASSERT_CHECKING
static void
prewrite_image_check(Page p)
{
	if (!O_PAGE_IS(p, LEAF))
	{
		BTreePageItemLocator loc;

		BTREE_PAGE_FOREACH_ITEMS(p, &loc)
		{
			BTreeNonLeafTuphdr *tuphdr = (BTreeNonLeafTuphdr *) BTREE_PAGE_LOCATOR_GET_ITEM(p, &loc);

			Assert(DOWNLINK_IS_ON_DISK(tuphdr->downlink));
		}
	}
}
#endif

/*
 * Returns downlink to the page or InvalidDiskDownlink if fails.
 */
uint64
perform_page_io(BTreeDescr *desc, OInMemoryBlkno blkno,
				Page img, uint32 checkpoint_number, bool copy_blkno,
				bool *dirty_parent)
{
	Page		page = O_GET_IN_MEMORY_PAGE(blkno);
	BTreePageHeader *header = (BTreePageHeader *) page;
	OrioleDBPageDesc *page_desc = O_GET_IN_MEMORY_PAGEDESC(blkno);
	Pointer		write_img;
	size_t		write_size;
	int			chkp_index;
	bool		less_num,
				err = false;

#ifdef USE_ASSERT_CHECKING
	prewrite_image_check(img);
#endif

	EA_WRITE_INC(blkno);

	less_num = header->checkpointNum < checkpoint_number;
	if (less_num)
	{
		/*
		 * Page wasn't yet written during given checkpoint, so we have to
		 * relocate it in order to implement copy-on-write checkpointing.
		 */
		if ((uintptr_t) page != (uintptr_t) img)
		{
			/*
			 * we need to update the written checkpoint number for the img too
			 */
			header = (BTreePageHeader *) img;
			header->checkpointNum = checkpoint_number;
			header = (BTreePageHeader *) page;
		}
		header->checkpointNum = checkpoint_number;
	}
	else
	{
		Assert(header->checkpointNum == checkpoint_number);
	}

	write_img = get_write_img(desc, img, &write_size);

	/*
	 * Determine the file position to write this page.
	 */
	chkp_index = checkpoint_number % 2;
	if (orioledb_s3_mode)
	{
		if (less_num)
		{
			err = !get_free_disk_extent(desc, checkpoint_number, write_size, &page_desc->fileExtent);
			*dirty_parent = true;
		}
		else
		{
			if (!OCompressIsValid(desc->compress))
			{
				/* easy case: no compression */
				*dirty_parent = false;
			}
			else
			{
				uint16		old_len = page_desc->fileExtent.len,
							new_len = FileExtentLen(write_size);

				if (old_len < new_len)
				{
					err = !get_free_disk_extent(desc, checkpoint_number, write_size, &page_desc->fileExtent);
					*dirty_parent = true;
				}
				else if (old_len > new_len)
				{
					page_desc->fileExtent.len = new_len;
					*dirty_parent = true;
				}
				else
				{
					*dirty_parent = false;
				}
			}
		}
	}
	else if (less_num)
	{
		/*
		 * Page wasn't yet written during given checkpoint, so we have to
		 * relocate it in order to implement copy-on-write checkpointing.
		 */

		if (FileExtentIsValid(page_desc->fileExtent))
		{
#ifdef USE_ASSERT_CHECKING
			/*
			 * Shared seq_bufs should be initialized by checkpointer.
			 */
			if (desc->storageType != BTreeStorageTemporary)
			{
				SpinLockAcquire(&desc->nextChkp[chkp_index].shared->lock);
				Assert(desc->nextChkp[chkp_index].shared->tag.num == checkpoint_number);
				SpinLockRelease(&desc->nextChkp[chkp_index].shared->lock);
			}
			SpinLockAcquire(&desc->tmpBuf[chkp_index].shared->lock);
			Assert(desc->tmpBuf[chkp_index].shared->tag.num == checkpoint_number);
			SpinLockRelease(&desc->tmpBuf[chkp_index].shared->lock);
#endif
			free_extent_for_checkpoint(desc, &page_desc->fileExtent, checkpoint_number);
		}

		/* Get free disk page to locate new page image */
		if (copy_blkno)
		{
			err = !get_free_disk_extent_copy_blkno(desc, write_size,
												   &page_desc->fileExtent,
												   checkpoint_number);
		}
		else
		{
			err = !get_free_disk_extent(desc, checkpoint_number, write_size, &page_desc->fileExtent);
		}

		*dirty_parent = true;
	}
	else
	{
		/*
		 * Has been already written during given checkpoint, so rewrite page
		 * in-place.
		 */
		Assert(FileExtentIsValid(page_desc->fileExtent));
		if (!OCompressIsValid(desc->compress))
		{
			/* easy case: no compression */
			*dirty_parent = false;
		}
		else
		{
			uint16		old_len = page_desc->fileExtent.len,
						new_len = FileExtentLen(write_size);

			/*
			 * check: is current image take as much space as previous written
			 * page?
			 */
			if (old_len < new_len)
			{
				free_extent_for_checkpoint(desc, &page_desc->fileExtent, checkpoint_number);
				/* allocate more file blocks */
				if (copy_blkno)
				{
					err = !get_free_disk_extent_copy_blkno(desc, write_size,
														   &page_desc->fileExtent,
														   checkpoint_number);
				}
				else
				{
					err = !get_free_disk_extent(desc, checkpoint_number,
												write_size, &page_desc->fileExtent);
				}
			}
			else if (old_len > new_len)
			{
				/*
				 * free space
				 */
				FileExtent	free_extent;

				free_extent.len = page_desc->fileExtent.len - new_len;
				free_extent.off = page_desc->fileExtent.off + new_len;

				if (!seq_buf_write_file_extent(&desc->nextChkp[chkp_index], free_extent) ||
					!seq_buf_write_file_extent(&desc->tmpBuf[chkp_index], free_extent))
				{
					err = true;
				}
				page_desc->fileExtent.len = new_len;
			}

			*dirty_parent = old_len != new_len;
		}
	}

	if (err)
	{
		ereport(PANIC, (errcode_for_file_access(),
						errmsg("could not (re) allocate file blocks for page %d to file %s",
							   blkno, btree_smgr_filename(desc, 0, checkpoint_number))));
	}

	Assert(FileExtentIsValid(page_desc->fileExtent));

	if (!write_page_to_disk(desc, &page_desc->fileExtent, write_img, write_size))
	{
		ereport(PANIC, (errcode_for_file_access(),
						errmsg("could not write page %d to file %s with offset %lu",
							   blkno,
							   btree_smgr_filename(desc, page_desc->fileExtent.off, checkpoint_number),
							   (unsigned long) page_desc->fileExtent.off)));

		return InvalidDiskDownlink;
	}

	return MAKE_ON_DISK_DOWNLINK(page_desc->fileExtent);
}

/*
 * Performs page write for autonomous checkpoint images.
 *
 * Returns downlink to the page.
 */
uint64
perform_page_io_autonomous(BTreeDescr *desc, uint32 chkpNum, Page img, FileExtent *extent)
{
	Pointer		write_img;
	size_t		write_size;

#ifdef USE_ASSERT_CHECKING
	prewrite_image_check(img);
#endif

	write_img = get_write_img(desc, img, &write_size);

	if (!get_free_disk_extent(desc, chkpNum, write_size, extent))
	{
		ereport(PANIC, (errcode_for_file_access(),
						errmsg("could not get free file offset for write page to file %s",
							   btree_smgr_filename(desc, 0, 0))));

		return InvalidDiskDownlink;
	}

	Assert(FileExtentIsValid(*extent));

	if (!write_page_to_disk(desc, extent, write_img, write_size))
	{
		uint64		offset;

		if (orioledb_s3_mode)
		{
			offset = extent->off & S3_OFFSET_MASK;
			chkpNum = S3_GET_CHKP_NUM(extent->off);
		}
		else
		{
			offset = extent->off;
			chkpNum = 0;
		}

		ereport(PANIC, (errcode_for_file_access(),
						errmsg("could not write autonomous page to file %s with offset %lu",
							   btree_smgr_filename(desc, offset, chkpNum),
							   (unsigned long) offset)));

		return InvalidDiskDownlink;
	}

	return MAKE_ON_DISK_DOWNLINK(*extent);
}

/*
 * Performs page write for tree build.
 *
 * Returns downlink to the page.
 */
uint64
perform_page_io_build(BTreeDescr *desc, Page img,
					  FileExtent *extent, BTreeMetaPage *metaPage)
{
	Pointer		write_img;
	size_t		write_size;
	uint32		chkpNum;

	btree_page_update_max_key_len(desc, img);

#ifdef USE_ASSERT_CHECKING
	prewrite_image_check(img);
#endif

	write_img = get_write_img(desc, img, &write_size);

	if (orioledb_s3_mode)
	{
		/* FIXME: concurrency */
		chkpNum = checkpoint_state->lastCheckpointNumber + 1;
	}
	else
	{
		chkpNum = 0;
	}

	if (!OCompressIsValid(desc->compress))
	{
		Assert(write_size == ORIOLEDB_BLCKSZ);

		extent->len = 1;
		if (use_device)
			extent->off = orioledb_device_alloc(desc, ORIOLEDB_BLCKSZ) / ORIOLEDB_COMP_BLCKSZ;
		else
			extent->off = pg_atomic_fetch_add_u64(&metaPage->datafileLength[chkpNum % 2], 1);
	}
	else
	{
		extent->len = FileExtentLen(write_size);
		if (use_device)
			extent->off = orioledb_device_alloc(desc, ORIOLEDB_BLCKSZ) / ORIOLEDB_COMP_BLCKSZ;
		else
			extent->off = pg_atomic_fetch_add_u64(&metaPage->datafileLength[chkpNum % 2], extent->len);
	}

	Assert(FileExtentIsValid(*extent));

	if (!write_page_to_disk(desc, extent, write_img, write_size))
	{
		ereport(PANIC, (errcode_for_file_access(),
						errmsg("could not write autonomous page to file %s with offset %lu",
							   btree_smgr_filename(desc, extent[0].off, chkpNum),
							   (unsigned long) extent[0].off)));

		return InvalidDiskDownlink;
	}

	return MAKE_ON_DISK_DOWNLINK(*extent);
}

/*
 * Prepare internal page for writing to disk.
 */
static bool
prepare_non_leaf_page(Page p)
{
	BTreePageItemLocator loc;

	BTREE_PAGE_FOREACH_ITEMS(p, &loc)
	{
		BTreeNonLeafTuphdr *tuphdr = (BTreeNonLeafTuphdr *) BTREE_PAGE_LOCATOR_GET_ITEM(p, &loc);

		if (DOWNLINK_IS_IN_IO(tuphdr->downlink))
			return false;

		if (DOWNLINK_IS_IN_MEMORY(tuphdr->downlink))
		{
			OInMemoryBlkno child = DOWNLINK_GET_IN_MEMORY_BLKNO(tuphdr->downlink);
			OrioleDBPageDesc *desc = O_GET_IN_MEMORY_PAGEDESC(child);

			/*
			 * It's worth less to write non-leaf page, if it's going to anyway
			 * become dirty after writing of child.
			 */
			if (IS_DIRTY(child))
				return false;

			/* XXX: should we also consider checkpoint number of child page? */
			tuphdr->downlink = MAKE_ON_DISK_DOWNLINK(desc->fileExtent);
		}
	}

	PAGE_SET_N_ONDISK(p, BTREE_PAGE_ITEMS_COUNT(p));
	return true;
}

/*
 * Evict the page, assuming target page and its parent are locked.
 */
static void
write_page(OBTreeFindPageContext *context, OInMemoryBlkno blkno, Page img,
		   uint32 checkpoint_number,
		   bool evict, bool copy_blkno)
{
	BTreeDescr *desc = context->desc;
	OInMemoryBlkno parent_blkno = OInvalidInMemoryBlkno;
	Page		parent_page = NULL;
	Page		p = O_GET_IN_MEMORY_PAGE(blkno);
	BTreePageItemLocator *parent_loc;
	int			ionum = -1,
				context_index;
	BTreeNonLeafTuphdr *int_hdr = NULL;
	uint32		parent_change_count = 0;
	OrioleDBPageDesc *page_desc = O_GET_IN_MEMORY_PAGEDESC(blkno);
	bool		is_root = desc->rootInfo.rootPageBlkno == blkno;

	/* rootPageBlkno can not be evicted here */
	Assert(!evict || !is_root);
	Assert(OInMemoryBlknoIsValid(desc->rootInfo.rootPageBlkno));
	Assert(page_is_locked(blkno));
	EA_EVICT_INC(blkno);

	if (!is_root)
	{
		context_index = context->index;
		parent_blkno = context->items[context_index].blkno;
		parent_loc = &context->items[context_index].locator;
		parent_change_count = context->items[context_index].pageChangeCount;

		parent_page = O_GET_IN_MEMORY_PAGE(parent_blkno);

		ionum = assign_io_num(parent_blkno, BTREE_PAGE_LOCATOR_GET_OFFSET(parent_page, parent_loc));

		/* Prepare to modify downlink in parent page */
		page_block_reads(parent_blkno);
		int_hdr = (BTreeNonLeafTuphdr *) BTREE_PAGE_LOCATOR_GET_ITEM(parent_page, parent_loc);
	}
	else
	{
		/*
		 * Root page still need ionum to prevent changing of checkpoint
		 * number.
		 */
		ionum = assign_io_num(blkno, MaxOffsetNumber);
	}

	if (!IS_DIRTY(blkno))
	{
		Assert(evict);

		/*
		 * Easy case: page isn't dirty and doesn't need to be written to the
		 * disk.  Then we just have to change downlink in the parent.
		 */
		int_hdr->downlink = MAKE_ON_DISK_DOWNLINK(page_desc->fileExtent);
		PAGE_INC_N_ONDISK(parent_page);

		/* Concurrent readers should give up when we release the lock... */
		O_PAGE_CHANGE_COUNT_INC(p);
		unlock_page(blkno);
		unlock_io(ionum);
	}
	else
	{
		uint64		new_downlink,
					old_downlink = 0;
		bool		dirty_parent;

		/* Mark parent downlink as IO in-progress. */
		if (evict)
		{
			old_downlink = int_hdr->downlink;
			int_hdr->downlink = MAKE_IO_DOWNLINK(ionum);
			O_PAGE_CHANGE_COUNT_INC(p);
		}
		/* Caller (walk_page()) ensured that there is no IO in progress */
		Assert(page_desc->ionum < 0);
		page_desc->ionum = ionum;
		if (!is_root)
			unlock_page(parent_blkno);

		/* Perform actual IO */
		if (evict)
		{
			unlock_page(blkno);
			new_downlink = perform_page_io(desc, blkno, p,
										   checkpoint_number, copy_blkno, &dirty_parent);

			if (DiskDownlinkIsValid(new_downlink))
				writeback_put_extent(&io_writeback, desc, new_downlink);

			/* Page is not dirty anymore */
			CLEAN_DIRTY(desc->ppool, blkno);
		}
		else
		{
			/* Non-leaf pages are already copied by caller */
			if (O_PAGE_IS(p, LEAF))
				memcpy(img, p, ORIOLEDB_BLCKSZ);

			CLEAN_DIRTY_CONCURRENT(blkno);
			unlock_page(blkno);

			if (STOPEVENTS_ENABLED())
			{
				Jsonb	   *params;

				params = btree_page_stopevent_params(desc, p);
				STOPEVENT(STOPEVENT_AFTER_IONUM_SET, params);
			}
			new_downlink = perform_page_io(desc, blkno, img,
										   checkpoint_number, copy_blkno, &dirty_parent);

			if (DiskDownlinkIsValid(new_downlink))
				writeback_put_extent(&io_writeback, desc, new_downlink);

			/* Clean dirty only if there are no concurrent writes */
			lock_page(blkno);
			if (!IS_DIRTY_CONCURRENT(blkno))
				CLEAN_DIRTY(desc->ppool, blkno);
			unlock_page(blkno);

			if (!DiskDownlinkIsValid(new_downlink))
			{
				page_desc->ionum = -1;
				unlock_io(ionum);
				ereport(ERROR, (errcode_for_file_access(),
								errmsg("could not evict page %d to disk", blkno)));
			}
			else if (!dirty_parent)
			{
				page_desc->ionum = -1;
				unlock_io(ionum);
				perform_writeback(&io_writeback);
				return;
			}
		}

		if (!is_root)
		{
			/* Refind parent */
			BTREE_PAGE_FIND_SET(context, DOWNLINK_LOCATION);
			if (O_PAGE_IS(p, RIGHTMOST))
			{
				refind_page(context, NULL, BTreeKeyRightmost,
							PAGE_GET_LEVEL(p) + 1,
							parent_blkno, parent_change_count);
			}
			else
			{
				OTuple		hikey;

				BTREE_PAGE_GET_HIKEY(hikey, p);
				refind_page(context, &hikey, BTreeKeyPageHiKey, PAGE_GET_LEVEL(p) + 1,
							parent_blkno, parent_change_count);
			}
			BTREE_PAGE_FIND_UNSET(context, DOWNLINK_LOCATION);

			context_index = context->index;
			parent_blkno = context->items[context_index].blkno;
			parent_loc = &context->items[context_index].locator;
			parent_change_count = context->items[context_index].pageChangeCount;

			/* Replace parent downlink with on-disk link */
			parent_page = O_GET_IN_MEMORY_PAGE(parent_blkno);
			page_block_reads(parent_blkno);
			int_hdr = (BTreeNonLeafTuphdr *) BTREE_PAGE_LOCATOR_GET_ITEM(parent_page, parent_loc);

			if (!DiskDownlinkIsValid(new_downlink))
			{
				/* error happens on write, rollback changes in shared memory */
				if (evict)
					int_hdr->downlink = old_downlink;
				page_desc->ionum = -1;
				unlock_io(ionum);
				unlock_page(parent_blkno);
				ereport(ERROR, (errcode_for_file_access(),
								errmsg("could not evict page %d to disk", blkno)));
			}
			else
			{
				if (dirty_parent)
					MARK_DIRTY(desc->ppool, parent_blkno);

				if (evict)
				{
					int_hdr->downlink = new_downlink;
					PAGE_INC_N_ONDISK(parent_page);
				}
			}
		}
		page_desc->ionum = -1;
		unlock_io(ionum);
	}

	if (!is_root)
		unlock_page(parent_blkno);

	if (evict)
		ppool_free_page(desc->ppool, blkno, NULL);

	perform_writeback(&io_writeback);
}

static void
btree_finalize_private_seq_bufs(BTreeDescr *desc, EvictedTreeData *evicted_data)
{
	int			chkp_index;
	bool		is_compressed = OCompressIsValid(desc->compress);

	chkp_index = SEQ_BUF_SHARED_EXIST(desc->nextChkp[0].shared) ? 0 : 1;

	/* we must do not evict BTree under checkpoint */
	Assert(!SEQ_BUF_SHARED_EXIST(desc->nextChkp[1 - chkp_index].shared));
	Assert(!SEQ_BUF_SHARED_EXIST(desc->tmpBuf[1 - chkp_index].shared));
	Assert(is_compressed || SEQ_BUF_SHARED_EXIST(desc->freeBuf.shared));
	Assert(SEQ_BUF_SHARED_EXIST(desc->nextChkp[chkp_index].shared));
	Assert(SEQ_BUF_SHARED_EXIST(desc->tmpBuf[chkp_index].shared));

	if (is_compressed)
	{
		evicted_data->freeBuf.tag = desc->freeBuf.tag;
		evicted_data->freeBuf.offset = 0;
	}
	else
	{
		evicted_data->freeBuf.tag = desc->freeBuf.shared->tag;
		evicted_data->freeBuf.offset = seq_buf_finalize(&desc->freeBuf);
		FREE_PAGE_IF_VALID(desc->ppool, desc->freeBuf.shared->pages[0]);
		FREE_PAGE_IF_VALID(desc->ppool, desc->freeBuf.shared->pages[1]);
	}

	evicted_data->nextChkp.tag = desc->nextChkp[chkp_index].shared->tag;
	evicted_data->nextChkp.offset = seq_buf_finalize(&desc->nextChkp[chkp_index]);
	FREE_PAGE_IF_VALID(desc->ppool, desc->nextChkp[chkp_index].shared->pages[0]);
	FREE_PAGE_IF_VALID(desc->ppool, desc->nextChkp[chkp_index].shared->pages[1]);

	evicted_data->tmpBuf.tag = desc->tmpBuf[chkp_index].shared->tag;
	evicted_data->tmpBuf.offset = seq_buf_finalize(&desc->tmpBuf[chkp_index]);
	FREE_PAGE_IF_VALID(desc->ppool, desc->tmpBuf[chkp_index].shared->pages[0]);
	FREE_PAGE_IF_VALID(desc->ppool, desc->tmpBuf[chkp_index].shared->pages[1]);
}

/*
 * Evict the tree, assuming rootPageBlkno page is locked.
 */
static void
evict_btree(BTreeDescr *desc, uint32 checkpoint_number)
{
	OInMemoryBlkno root_blkno = desc->rootInfo.rootPageBlkno;
	Page		rootPageBlkno = O_GET_IN_MEMORY_PAGE(root_blkno);
	OrioleDBPageDesc *root_desc = O_GET_IN_MEMORY_PAGEDESC(root_blkno);
	BTreeMetaPage *metaPage = BTREE_GET_META(desc);
	CheckpointFileHeader file_header = {0};
	EvictedTreeData evicted_tree_data = {{0}};
	uint64		new_downlink;
	char	   *filename,
				img[ORIOLEDB_BLCKSZ];
	File		file;
	bool		was_dirty;
	int			i PG_USED_FOR_ASSERTS_ONLY;
	uint32		chkpNum = 0;

	Assert(ORootPageIsValid(desc) && OMetaPageIsValid(desc) &&
		   O_PAGE_STATE_IS_LOCKED(pg_atomic_read_u32(&(O_PAGE_HEADER(rootPageBlkno)->state))));

	/* we check it before */
	Assert(!RightLinkIsValid(BTREE_PAGE_GET_RIGHTLINK(rootPageBlkno)));
	if (orioledb_s3_mode)
	{
		btree_s3_flush(desc, 0);
		btree_s3_flush(desc, 1);
	}

	was_dirty = IS_DIRTY(root_blkno);
	if (was_dirty)
	{
		bool		not_used;

		CLEAN_DIRTY(desc->ppool, root_blkno);

		/* Code above ensured there is no IO in progress */
		Assert(root_desc->ionum < 0);
		root_desc->ionum = assign_io_num(root_blkno, InvalidOffsetNumber);
		memcpy(img, rootPageBlkno, ORIOLEDB_BLCKSZ);
		unlock_page(root_blkno);

		new_downlink = perform_page_io(desc, root_blkno, img, checkpoint_number,
									   false, &not_used);
		if (!DiskDownlinkIsValid(new_downlink))
		{
			elog(FATAL, "Can not evict rootPageBlkno page on disk.");
		}

		writeback_put_extent(&io_writeback, desc, new_downlink);
		unlock_io(root_desc->ionum);
		root_desc->ionum = -1;
	}
	else
	{
		new_downlink = MAKE_ON_DISK_DOWNLINK(root_desc->fileExtent);
		unlock_page(root_blkno);
	}

	file_header.rootDownlink = new_downlink;

	ppool_free_page(desc->ppool, root_blkno, NULL);

	if (orioledb_s3_mode)
		chkpNum = S3_GET_CHKP_NUM(DOWNLINK_GET_DISK_OFF(new_downlink));

	file_header.datafileLength = pg_atomic_read_u64(&metaPage->datafileLength[chkpNum % 2]);
	file_header.leafPagesNum = pg_atomic_read_u32(&metaPage->leafPagesNum);
	file_header.ctid = pg_atomic_read_u64(&metaPage->ctid);
	file_header.numFreeBlocks = pg_atomic_read_u64(&metaPage->numFreeBlocks);
#ifdef USE_ASSERT_CHECKING
	for (i = 0; i < NUM_SEQ_SCANS_ARRAY_SIZE; i++)
		Assert(pg_atomic_read_u32(&metaPage->numSeqScans[i]) == 0);
#endif

	evicted_tree_data.file_header = file_header;
	evicted_tree_data.maxLocation[0] = metaPage->partsInfo[0].writeMaxLocation;
	evicted_tree_data.maxLocation[1] = metaPage->partsInfo[1].writeMaxLocation;

	/*
	 * Free all private seq buf pages and get their offsets
	 */
	btree_finalize_private_seq_bufs(desc, &evicted_tree_data);
	filename = get_eviction_filename(desc->oids, checkpoint_number);

	file = PathNameOpenFile(filename, O_WRONLY | O_CREAT | PG_BINARY);
	if (file < 0)
	{
		ereport(FATAL, (errcode_for_file_access(),
						errmsg("Could not open eviction file: %s",
							   filename)));
	}

	if (OFileWrite(file,
				   (Pointer) &evicted_tree_data,
				   sizeof(evicted_tree_data),
				   0,
				   WAIT_EVENT_DATA_FILE_WRITE) != sizeof(evicted_tree_data))
	{
		ereport(FATAL, (errcode_for_file_access(),
						errmsg("Could not write eviction data to file: %s",
							   filename)));
	}

	FileClose(file);
	pfree(filename);

	ppool_free_page(desc->ppool, desc->rootInfo.metaPageBlkno, NULL);

	desc->rootInfo.rootPageBlkno = OInvalidInMemoryBlkno;
	desc->rootInfo.metaPageBlkno = OInvalidInMemoryBlkno;

	perform_writeback(&io_writeback);

	/*
	 * Shared descr drops to signalize other backends that tree is evicted.
	 * Backends and workers can create a new SharedRootInfo* after this.
	 */
	o_drop_shared_root_info(desc->oids.datoid, desc->oids.relnode);
}

BTreeDescr *
index_oids_get_btree_descr(ORelOids oids, OIndexType type)
{
	OIndexDescr *indexDescr = NULL;
	BTreeDescr *desc;
	bool		nested;

	/* Check is this table is visible for us */
	indexDescr = o_fetch_index_descr(oids, type, false, &nested);

	if (indexDescr == NULL)
		return NULL;

	desc = &indexDescr->desc;

	if (!o_btree_try_use_shmem(desc))
		return NULL;

	return desc;
}

/*
 * Examine single page and evict it if possible.
 */
OWalkPageResult
walk_page(OInMemoryBlkno blkno, bool evict)
{
	OrioleDBPageDesc *page_desc = O_GET_IN_MEMORY_PAGEDESC(blkno);
	OBTreeFindPageContext context;
	BTreeDescr *desc;
	Page		p = O_GET_IN_MEMORY_PAGE(blkno),
				parent_page;
	ORelOids	oids;
	BTreeNonLeafTuphdr *int_hdr;
	uint32		checkpoint_number;
	bool		copy_blkno,
				found,
				merge_tried = false;
	int			ionum;
	char		img[ORIOLEDB_BLCKSZ];
	bool		is_root;

	p = O_GET_IN_MEMORY_PAGE(blkno);
retry:

	if (!ORelOidsIsValid(page_desc->oids) || page_desc->type == oIndexInvalid)
		return OWalkPageSkipped;

	if (!O_PAGE_IS(p, LEAF) && evict && PAGE_GET_N_ONDISK(p) != BTREE_PAGE_ITEMS_COUNT(p))
		return OWalkPageSkipped;

	if (!evict && !IS_DIRTY(blkno))
		return OWalkPageSkipped;

	if (!try_lock_page(blkno))
		return OWalkPageSkipped;

	/* page is locked once we get here */

	if (!ORelOidsIsValid(page_desc->oids) || page_desc->type == oIndexInvalid)
	{
		unlock_page(blkno);
		return OWalkPageSkipped;
	}

	if (!evict && !IS_DIRTY(blkno))
	{
		unlock_page(blkno);
		return OWalkPageSkipped;
	}

	if (O_PAGE_IS(p, PRE_CLEANUP))
	{
		unlock_page(blkno);
		return OWalkPageSkipped;
	}

	/* On concurrent IO, then wait for completion and retry */
	ionum = page_desc->ionum;
	if (ionum >= 0)
	{
		unlock_page(blkno);
		wait_for_io_completion(ionum);
		goto retry;
	}

	if (!O_PAGE_IS(p, LEAF) && evict && PAGE_GET_N_ONDISK(p) != BTREE_PAGE_ITEMS_COUNT(p))
	{
		unlock_page(blkno);
		return OWalkPageSkipped;
	}

	if (!O_PAGE_IS(p, LEAF) && !evict)
	{
		memcpy(img, p, ORIOLEDB_BLCKSZ);
		if (!prepare_non_leaf_page(img))
		{
			unlock_page(blkno);
			return OWalkPageSkipped;
		}
	}

	if (RightLinkIsValid(BTREE_PAGE_GET_RIGHTLINK(p)))
	{
		unlock_page(blkno);
		return OWalkPageSkipped;
	}

	oids = page_desc->oids;
	if (IS_SYS_TREE_OIDS(oids))
	{
		if (sys_tree_get_storage_type(oids.relnode) != BTreeStorageInMemory)
		{
			desc = get_sys_tree(oids.relnode);
		}
		else
		{
			unlock_page(blkno);
			return OWalkPageSkipped;
		}
	}
	else
	{
		/* Check is this index is visible for us */
		desc = index_oids_get_btree_descr(oids, page_desc->type);

		if (desc == NULL)
		{
			unlock_page(blkno);
			return OWalkPageSkipped;
		}
	}

	/* Try to merge sparse page instead of eviction */
	if (!merge_tried && is_page_too_sparse(desc, p))
	{
		bool		result;

		result = btree_try_merge_and_unlock(desc, blkno, true, false);

		/* Merge shouldn't leave us with locked pages. */
		Assert(!have_locked_pages());

		if (result)
		{
			return OWalkPageMerged;
		}
		else
		{
			merge_tried = true;
			goto retry;
		}
	}

	Assert(desc != NULL);
	Assert(ORootPageIsValid(desc) && OMetaPageIsValid(desc));
	is_root = desc->rootInfo.rootPageBlkno == blkno;

	/* If page is rootPageBlkno, we don't need to search parent page. */
	context.desc = desc;
	context.index = 0;
	if (!is_root)
	{
		init_page_find_context(&context, desc, COMMITSEQNO_INPROGRESS, BTREE_PAGE_FIND_MODIFY
							   | BTREE_PAGE_FIND_TRY_LOCK
							   | BTREE_PAGE_FIND_DOWNLINK_LOCATION
							   | (evict ? BTREE_PAGE_FIND_NO_FIX_SPLIT : 0));
		if (O_PAGE_IS(p, RIGHTMOST))
		{
			found = find_page(&context, NULL, BTreeKeyRightmost, PAGE_GET_LEVEL(p) + 1);
		}
		else
		{
			OTuple		hikey;

			BTREE_PAGE_GET_HIKEY(hikey, p);
			found = find_page(&context, &hikey, BTreeKeyPageHiKey, PAGE_GET_LEVEL(p) + 1);
		}

		if (!found)
		{
			unlock_page(blkno);
			Assert(!have_locked_pages());
			return OWalkPageSkipped;
		}

		BTREE_PAGE_FIND_UNSET(&context, TRY_LOCK);
		parent_page = O_GET_IN_MEMORY_PAGE(context.items[context.index].blkno);

		int_hdr = (BTreeNonLeafTuphdr *) BTREE_PAGE_LOCATOR_GET_ITEM(parent_page, &context.items[context.index].locator);

		if (!DOWNLINK_IS_IN_MEMORY(int_hdr->downlink) ||
			DOWNLINK_GET_IN_MEMORY_BLKNO(int_hdr->downlink) != blkno)
		{
			/*
			 * We didn't find downlink pointing to this page.  This could
			 * happend because of concurrent split.  Give up then...
			 */
			unlock_page(blkno);
			unlock_page(context.items[context.index].blkno);
			return OWalkPageSkipped;
		}
	}
	else if (IS_SYS_TREE_OIDS(oids))
	{
		Assert(is_root);
		unlock_page(blkno);
		return OWalkPageSkipped;
	}

	if (!get_checkpoint_number(desc, blkno, &checkpoint_number, &copy_blkno))
	{
		unlock_page(blkno);

		if (!is_root)
			unlock_page(context.items[context.index].blkno);
		return OWalkPageSkipped;
	}

	if (evict && is_root)
	{
		bool		recovery = is_recovery_in_progress();
		bool		acquired;
		bool		nested = false;

		if (tree_is_under_checkpoint(desc))
		{
			unlock_page(blkno);
			return OWalkPageSkipped;
		}

		if (!recovery)
			acquired = o_tables_rel_try_lock_extended(&oids, AccessExclusiveLock, &nested, false);
		else
			acquired = true;

		if (acquired)
		{
			if (!nested &&
				o_tables_rel_try_lock_extended(&oids, AccessExclusiveLock, &nested, true))
			{
				bool		result = false;

				if (!nested)
				{
					/*
					 * Descriptor might be already invalidated.
					 */
					desc = index_oids_get_btree_descr(oids, page_desc->type);

					if (desc != NULL &&
						desc->rootInfo.rootPageBlkno == blkno)
					{
						evict_btree(desc, checkpoint_number);
						o_invalidate_oids(oids);
						result = true;
					}
					else
					{
						unlock_page(blkno);
					}
				}
				else
				{
					unlock_page(blkno);
				}

				if (!recovery)
					o_tables_rel_unlock_extended(&oids, AccessExclusiveLock, false);
				o_tables_rel_unlock_extended(&oids, AccessExclusiveLock, true);
				return result ? OWalkPageEvicted : OWalkPageSkipped;
			}
			else
			{
				if (!recovery)
					o_tables_rel_unlock_extended(&oids, AccessExclusiveLock, false);
			}
		}
		unlock_page(blkno);
		return OWalkPageSkipped;
	}

	STOPEVENT(STOPEVENT_BEFORE_WRITE_PAGE, NULL);

	write_page(&context, blkno, img, checkpoint_number, evict, copy_blkno);

	STOPEVENT(STOPEVENT_AFTER_WRITE_PAGE, NULL);

	return evict ? OWalkPageEvicted : OWalkPageWritten;
}

static bool
write_tree_pages_recursive(OInMemoryBlkno blkno, uint32 changeCount,
						   int maxLevel, bool evict)
{
	Page		p;
	int			level;
	OInMemoryBlkno childPageNumbers[BTREE_PAGE_MAX_CHUNK_ITEMS];
	uint32		childPageChangeCounts[BTREE_PAGE_MAX_CHUNK_ITEMS];
	int			childPagesCount = 0;
	int			i;
	BTreePageItemLocator loc;

	if (!OInMemoryBlknoIsValid(blkno))
		return false;

	lock_page(blkno);
	p = O_GET_IN_MEMORY_PAGE(blkno);
	if (O_PAGE_GET_CHANGE_COUNT(p) != changeCount)
	{
		unlock_page(blkno);
		return false;
	}
	level = PAGE_GET_LEVEL(p);

	if (!O_PAGE_IS(p, LEAF))
	{
		BTREE_PAGE_FOREACH_ITEMS(p, &loc)
		{
			BTreeNonLeafTuphdr *tuphdr = (BTreeNonLeafTuphdr *) BTREE_PAGE_LOCATOR_GET_ITEM(p, &loc);

			if (DOWNLINK_IS_IN_MEMORY(tuphdr->downlink))
			{
				childPageNumbers[childPagesCount] = DOWNLINK_GET_IN_MEMORY_BLKNO(tuphdr->downlink);
				childPageChangeCounts[childPagesCount] = DOWNLINK_GET_IN_MEMORY_CHANGECOUNT(tuphdr->downlink);
				childPagesCount++;
			}
		}
	}

	unlock_page(blkno);

	for (i = 0; i < childPagesCount; i++)
		(void) write_tree_pages_recursive(childPageNumbers[i],
										  childPageChangeCounts[i],
										  maxLevel,
										  evict);

	if (level <= maxLevel)
	{
		while (true)
		{
			reserve_undo_size(UndoReserveTxn, 2 * O_MERGE_UNDO_IMAGE_SIZE);
			if (walk_page(blkno, evict) != OWalkPageMerged)
				break;
		}
		release_undo_size(UndoReserveTxn);
	}

	return true;
}

static void
write_tree_pages(BTreeDescr *desc, int maxLevel, bool evict)
{
	o_btree_load_shmem(desc);
	if (!write_tree_pages_recursive(desc->rootInfo.rootPageBlkno,
									desc->rootInfo.rootPageChangeCount,
									maxLevel, evict))
	{
		desc->rootInfo.rootPageBlkno = OInvalidInMemoryBlkno;
		desc->rootInfo.metaPageBlkno = OInvalidInMemoryBlkno;
		desc->rootInfo.rootPageChangeCount = 0;
		o_btree_load_shmem(desc);
		(void) write_tree_pages_recursive(desc->rootInfo.rootPageBlkno,
										  desc->rootInfo.rootPageChangeCount,
										  maxLevel, evict);
	}
}

static void
write_relation_pages(Oid relid, int maxLevel, bool evict)
{
	OTableDescr *descr;
	BTreeDescr *td;
	Relation	rel;
	int			treen;

	orioledb_check_shmem();

	rel = relation_open(relid, AccessShareLock);

	if (!rel)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("relation oid %u does not exists", relid)));

	descr = relation_get_descr(rel);

	for (treen = 0; treen < descr->nIndices; treen++)
	{
		td = &descr->indices[treen]->desc;
		write_tree_pages(td, maxLevel, evict);
	}
	td = &descr->toast->desc;
	write_tree_pages(td, maxLevel, evict);

	relation_close(rel, AccessShareLock);
}

Datum
orioledb_evict_pages(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	int			maxLevel = PG_GETARG_INT32(1);

	write_relation_pages(relid, maxLevel, true);

	PG_RETURN_VOID();
}

Datum
orioledb_write_pages(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	int			maxLevel = ORIOLEDB_MAX_DEPTH;

	write_relation_pages(relid, maxLevel, false);

	PG_RETURN_VOID();
}

static int
tree_offsets_cmp(const void *a, const void *b)
{
	TreeOffset	val1 = *(TreeOffset *) a;
	TreeOffset	val2 = *(TreeOffset *) b;

	if (val1.datoid != val2.datoid)
		return (val1.datoid < val2.datoid) ? -1 : 1;
	else if (val1.relnode != val2.relnode)
		return (val1.relnode < val2.relnode) ? -1 : 1;
	else if (val1.chkpNum != val2.chkpNum)
		return (val1.chkpNum < val2.chkpNum) ? -1 : 1;
	else if (val1.segno != val2.segno)
		return (val1.segno < val2.segno) ? -1 : 1;
	else if (val1.fileExtent.off != val2.fileExtent.off)
		return val1.fileExtent.off < val2.fileExtent.off ? -1 : 1;
	else if (val1.fileExtent.len != val2.fileExtent.len)
	{
		/*
		 * an extent with bigger length will be placed first, it helps to
		 * simplify process this case in perform_writeback()
		 */
		return val1.fileExtent.len > val2.fileExtent.len ? -1 : 1;
	}

	return 0;
}

static void
writeback_put_extent(IOWriteBack *writeback, BTreeDescr *desc,
					 uint64 downlink)
{
	TreeOffset	offset;
	off_t		blcksz = 0;
	int			last_segno;
	FileExtent	extent;

	Assert(DOWNLINK_IS_ON_DISK(downlink));
	extent.len = DOWNLINK_GET_DISK_LEN(downlink);
	extent.off = DOWNLINK_GET_DISK_OFF(downlink);

	if (!ORelOidsIsValid(desc->oids) || desc->type == oIndexInvalid)
		return;

	if (orioledb_s3_mode)
	{
		offset.chkpNum = S3_GET_CHKP_NUM(extent.off);
		extent.off &= S3_OFFSET_MASK;
	}
	else
	{
		offset.chkpNum = 0;
	}

	Assert(extent.len > 0);
	Assert(extent.len <= (ORIOLEDB_BLCKSZ / ORIOLEDB_COMP_BLCKSZ));

	offset.datoid = desc->oids.datoid;
	offset.relnode = desc->oids.relnode;
	offset.compressed = OCompressIsValid(desc->compress);
	blcksz = offset.compressed ? ORIOLEDB_COMP_BLCKSZ : ORIOLEDB_BLCKSZ;
	offset.segno = blcksz * extent.off / ORIOLEDB_SEGMENT_SIZE;
	last_segno = blcksz * (extent.off + extent.len - 1) / ORIOLEDB_SEGMENT_SIZE;

	while (offset.segno <= last_segno)
	{
		if (writeback->extents == NULL)
		{
			writeback->extentsNumber = 0;
			writeback->extentsAllocated = 16;
			writeback->extents = (TreeOffset *) MemoryContextAlloc(TopMemoryContext,
																   sizeof(TreeOffset) * writeback->extentsAllocated);
		}
		else if (writeback->extentsNumber >= writeback->extentsAllocated)
		{
			writeback->extentsAllocated *= 2;
			writeback->extents = (TreeOffset *) repalloc(writeback->extents,
														 sizeof(TreeOffset) * writeback->extentsAllocated);
		}

		offset.fileExtent = extent;
		if (offset.segno != last_segno)
			offset.fileExtent.len = ORIOLEDB_SEGMENT_SIZE / blcksz - extent.off % (ORIOLEDB_SEGMENT_SIZE / blcksz);
		writeback->extents[writeback->extentsNumber] = offset;
		writeback->extentsNumber++;
		offset.segno++;
		extent.off += offset.fileExtent.len;
		extent.len -= offset.fileExtent.len;
	}
}

static void
perform_writeback(IOWriteBack *writeback)
{
	int			i,
				len = 0,
				flushAfter;
	uint64		offset = InvalidFileExtentOff - 1;
	off_t		blcksz = 0;
	Oid			datoid = InvalidOid,
				relnode = InvalidOid;
	File		file = -1;
	int			segno = 0;
	int			chkpNum = 0;

	if (use_device && !use_mmap)
	{
		writeback->extentsNumber = 0;
		return;
	}

	flushAfter = IsBGWriter ? bgwriter_flush_after : backend_flush_after;
	flushAfter *= BLCKSZ / ORIOLEDB_BLCKSZ;

	if (writeback->extentsNumber < flushAfter)
		return;

	pg_qsort(writeback->extents, writeback->extentsNumber,
			 sizeof(TreeOffset), tree_offsets_cmp);

	for (i = 0; i < writeback->extentsNumber; i++)
	{
		TreeOffset	cur = writeback->extents[i];

		if (datoid != cur.datoid || relnode != cur.relnode ||
			segno != cur.segno || chkpNum != cur.chkpNum)
		{
			if (use_mmap)
			{
				if (len > 0)
					msync(mmap_data + (off_t) segno * ORIOLEDB_SEGMENT_SIZE + (off_t) offset * blcksz, (off_t) len * blcksz, MS_ASYNC);
			}
			else
			{
				if (len > 0)
				{
					FileWriteback(file, (off_t) offset * blcksz,
								  (off_t) len * blcksz,
								  WAIT_EVENT_DATA_FILE_FLUSH);
				}
				if (file >= 0)
					FileClose(file);
			}

			blcksz = cur.compressed ? ORIOLEDB_COMP_BLCKSZ : ORIOLEDB_BLCKSZ;
			datoid = cur.datoid;
			relnode = cur.relnode;
			segno = cur.segno;
			chkpNum = cur.chkpNum;
			if (!use_mmap)
			{
				char	   *filename;

				filename = btree_filename(datoid, relnode, segno, chkpNum);
				file = PathNameOpenFile(filename, O_RDWR | O_CREAT | PG_BINARY);
				pfree(filename);
				offset = cur.fileExtent.off;
				len = cur.fileExtent.len;
			}
		}
		else
		{
			if (cur.fileExtent.off == offset)
			{
				continue;
			}
			else if (cur.fileExtent.off == offset + len)
			{
				len += cur.fileExtent.len;
			}
			else
			{
				if (use_mmap)
					msync(mmap_data + (off_t) segno * ORIOLEDB_SEGMENT_SIZE + (off_t) offset * blcksz, (off_t) len * blcksz, MS_ASYNC);
				else
					FileWriteback(file, (off_t) offset * blcksz,
								  (off_t) len * blcksz,
								  WAIT_EVENT_DATA_FILE_FLUSH);
				offset = cur.fileExtent.off;
				len = cur.fileExtent.len;
			}
		}
	}

	if (len > 0)
	{
		Assert(blcksz != 0);
		if (use_mmap)
			msync(mmap_data + (off_t) segno * ORIOLEDB_SEGMENT_SIZE + (off_t) offset * blcksz, (off_t) len * blcksz, MS_ASYNC);
		else
			FileWriteback(file, (off_t) offset * blcksz,
						  (off_t) len * blcksz,
						  WAIT_EVENT_DATA_FILE_FLUSH);
	}

	if (!use_mmap && file >= 0)
		FileClose(file);

	writeback->extentsNumber = 0;
}

typedef void (*RelnodeFileCallback) (const char *filename, uint32 segno,
									 char *ext, void *arg);

/*
 * Iterate all the files belonging to given (datoid, relnode) pair and call
 * the callback for each filename.
 *
 * Guarantees that at first we process the first data file.
 */
static bool
iterate_relnode_files(Oid datoid, Oid relnode, RelnodeFileCallback callback,
					  void *arg)
{
	struct dirent *file;
	DIR		   *dir;
	char	   *filename;
	char	   *dirname;
	bool		first_file_deleted = false;

	dirname = psprintf(ORIOLEDB_DATA_DIR "/%u", datoid);

	dir = opendir(dirname);

	pfree(dirname);
	if (dir == NULL)
		return false;

	while (errno = 0, (file = readdir(dir)) != NULL)
	{
		uint32		file_relnode,
					file_chkp = 0,
					file_segno = 0;
		char		file_ext[5];
		char	   *file_ext_p = NULL;

		if ((sscanf(file->d_name, "%10u-%10u.%4s",
					&file_relnode, &file_chkp, file_ext) == 3 &&
			 (!strcmp(file_ext, "tmp") || !strcmp(file_ext, "map")) &&
			 (file_ext_p = file_ext)) ||
			sscanf(file->d_name, "%10u.%10u",
				   &file_relnode, &file_segno) == 2 ||
			sscanf(file->d_name, "%10u",
				   &file_relnode) == 1)
		{
			if (relnode == file_relnode)
			{
				if (!orioledb_s3_mode && !first_file_deleted)
				{
					filename = psprintf(ORIOLEDB_DATA_DIR "/%u/%u",
										datoid, relnode);
					callback(filename, 0, NULL, arg);
					pfree(filename);
					first_file_deleted = true;
				}

				if (file_segno != 0 || file_ext_p != NULL)
				{
					filename = psprintf(ORIOLEDB_DATA_DIR "/%u/%s",
										datoid, file->d_name);
					callback(filename, file_segno, file_ext_p, arg);
					pfree(filename);
				}
			}
		}
	}

	closedir(dir);
	return true;
}

static void
unlink_callback(const char *filename, uint32 segno, char *ext, void *arg)
{
	/*
	 * Recovery determines relation data presence by presence of the first
	 * data file.  So, we durably delete the first data file to evade
	 * situation when partially deleted file data is visible.
	 */
	if (segno == 0 && ext == NULL)
		durable_unlink(filename, ERROR);
	else
		unlink(filename);
}

bool
cleanup_btree_files(Oid datoid, Oid relnode)
{
	return iterate_relnode_files(datoid, relnode, unlink_callback, NULL);
}

static void
fsync_callback(const char *filename, uint32 segno, char *ext, void *arg)
{
	fsync_fname(filename, false);
}

bool
fsync_btree_files(Oid datoid, Oid relnode)
{
	return iterate_relnode_files(datoid, relnode, fsync_callback, NULL);
}
