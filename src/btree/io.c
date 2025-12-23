/*-------------------------------------------------------------------------
 *
 * io.c
 *		Routines for orioledb B-tree disk IO.
 *
 * Copyright (c) 2021-2025, Oriole DB Inc.
 * Copyright (c) 2025, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/btree/io.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <fcntl.h>
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
#include "catalog/o_sys_cache.h"
#include "recovery/recovery.h"
#include "s3/headers.h"
#include "s3/worker.h"
#include "tableam/descr.h"
#include "tableam/handler.h"
#include "utils/compress.h"
#include "utils/elog.h"
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
							   uint32 curChkpNum,
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
	uint32		loadId;
	char		status;			/* for simplehash use */
} FileHashElement;

#define SH_PREFIX s3Files
#define SH_ELEMENT_TYPE FileHashElement
#define SH_KEY_TYPE FileHashKey
#define SH_KEY key
#define SH_HASH_KEY(tb, key) hash_any((unsigned char *) &key, sizeof(FileHashKey))
#define SH_EQUAL(tb, a, b) memcmp(&a, &b, sizeof(FileHashKey)) == 0
#define SH_SCOPE static inline
#define SH_DEFINE
#define SH_DECLARE
#include "lib/simplehash.h"

char *
btree_filename(Oid datoid, Oid relnode, int segno, uint32 chkpNum)
{
	char	   *result;
	char	   *db_prefix;

	o_get_prefixes_for_relnode(datoid, relnode, NULL, &db_prefix);

	if (orioledb_s3_mode)
	{
		if (segno == 0)
			result = psprintf("%s/%u-%u",
							  db_prefix,
							  relnode,
							  chkpNum);
		else
			result = psprintf("%s/%u.%u-%u",
							  db_prefix,
							  relnode,
							  segno,
							  chkpNum);
	}
	else
	{
		if (segno == 0)
			result = psprintf("%s/%u",
							  db_prefix,
							  relnode);
		else
			result = psprintf("%s/%u.%u",
							  db_prefix,
							  relnode,
							  segno);
	}

	pfree(db_prefix);
	return result;
}

char *
btree_smgr_filename(BTreeDescr *desc, off_t offset, uint32 chkpNum)
{
	int			segno = offset / ORIOLEDB_SEGMENT_SIZE;

	return btree_filename(desc->oids.datoid,
						  desc->oids.relnode,
						  segno,
						  chkpNum);
}

static File
btree_open_smgr_file(BTreeDescr *desc, uint32 num, uint32 chkpNum,
					 uint32 loadId)
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
		{
			if (hashElem->loadId == loadId)
				return hashElem->file;
			else
				FileClose(hashElem->file);
		}

		filename = btree_smgr_filename(desc,
									   (off_t) num * ORIOLEDB_SEGMENT_SIZE,
									   chkpNum);
		hashElem->file = PathNameOpenFile(filename, O_RDWR | O_CREAT | PG_BINARY);
		hashElem->loadId = loadId;
		if (hashElem->file <= 0)
			ereport(FATAL,
					(errcode_for_file_access(),
					 errmsg("could not open data file %s: %m", filename)));
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
					 errmsg("could not open data file %s: %m", filename)));
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
		int			i;
		int			j;

		descr->smgr.hash = s3Files_create(TopMemoryContext, 16, NULL);

		for (i = 0; i < 2; i++)
		{
			descr->buildPartsInfo[i].writeMaxLocation = 0;
			for (j = 0; j < MAX_NUM_DIRTY_PARTS; j++)
			{
				descr->buildPartsInfo[i].dirtyParts[j].segNum = -1;
				descr->buildPartsInfo[i].dirtyParts[j].partNum = -1;
			}
		}
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
		(void) btree_open_smgr_file(descr, 0, 0, 0);
	}
}

void
btree_close_smgr(BTreeDescr *descr)
{
	int			i;

	if (orioledb_s3_mode)
	{
		int			j;

		for (j = 0; j < 2; j++)
		{
			for (i = 0; i < MAX_NUM_DIRTY_PARTS; i++)
			{
				S3TaskLocation location;
				uint32		chkpNum;
				int32		segNum,
							partNum;

				chkpNum = descr->buildPartsInfo[j].dirtyParts[i].chkpNum;
				segNum = descr->buildPartsInfo[j].dirtyParts[i].segNum;
				partNum = descr->buildPartsInfo[j].dirtyParts[i].partNum;
				if (segNum >= 0 && partNum >= 0)
				{
					location = s3_schedule_file_part_write(chkpNum,
														   descr->oids.datoid,
														   descr->oids.relnode,
														   segNum,
														   partNum);
					descr->buildPartsInfo[j].writeMaxLocation =
						Max(descr->buildPartsInfo[j].writeMaxLocation, location);
				}
				descr->buildPartsInfo[j].dirtyParts[i].chkpNum = 0;
				descr->buildPartsInfo[j].dirtyParts[i].segNum = -1;
				descr->buildPartsInfo[j].dirtyParts[i].partNum = -1;
			}
		}

		if (descr->smgr.hash)
		{
			s3Files_iterator i;
			FileHashElement *hashElem;

			s3Files_start_iterate(descr->smgr.hash, &i);
			while ((hashElem = s3Files_iterate(descr->smgr.hash, &i)) != NULL)
				FileClose(hashElem->file);

			s3Files_destroy(descr->smgr.hash);
		}
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
			Assert(chkpNum == meta->partsInfo[chkpNum % 2].dirtyParts[i].chkpNum);
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
				curChkpNum,
				tmpSegNum,
				tmpPartNum,
				tmpChkpNum;
	BTreeS3PartsInfo *partsInfo = NULL;

	if (OInMemoryBlknoIsValid(desc->rootInfo.metaPageBlkno))
	{
		BTreeMetaPage *meta = BTREE_GET_META(desc);

		partsInfo = meta->partsInfo;
	}
	else
	{
		partsInfo = desc->buildPartsInfo;
	}

	curSegNum = segNum;
	curPartNum = partNum;
	curChkpNum = chkpNum;
	for (i = 0; i < MAX_NUM_DIRTY_PARTS; i++)
	{
		tmpSegNum = partsInfo[chkpNum % 2].dirtyParts[i].segNum;
		tmpPartNum = partsInfo[chkpNum % 2].dirtyParts[i].partNum;
		tmpChkpNum = partsInfo[chkpNum % 2].dirtyParts[i].chkpNum;
		partsInfo[chkpNum % 2].dirtyParts[i].segNum = curSegNum;
		partsInfo[chkpNum % 2].dirtyParts[i].partNum = curPartNum;
		partsInfo[chkpNum % 2].dirtyParts[i].chkpNum = curChkpNum;
		curSegNum = tmpSegNum;
		curPartNum = tmpPartNum;
		curChkpNum = tmpChkpNum;

		if ((curSegNum == segNum &&
			 curPartNum == partNum &&
			 curChkpNum == chkpNum) ||
			curSegNum < 0)
			break;

		if (i == MAX_NUM_DIRTY_PARTS - 1)
		{
			S3TaskLocation location;

			location = s3_schedule_file_part_write(curChkpNum,
												   desc->oids.datoid,
												   desc->oids.relnode,
												   curSegNum,
												   curPartNum);
			partsInfo[chkpNum % 2].writeMaxLocation =
				Max(partsInfo[chkpNum % 2].writeMaxLocation, location);
		}
	}
}

static int
btree_smgr_write(BTreeDescr *desc, char *buffer, uint32 chkpNum,
				 int amount, off_t offset)
{
	int			result = 0;
	off_t		curOffset = offset,
				granularity;
	S3HeaderTag tag = {0};

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

	if (orioledb_s3_mode)
	{
		granularity = ORIOLEDB_S3_PART_SIZE;
		tag.datoid = desc->oids.datoid;
		tag.relnode = desc->oids.relnode;
		tag.checkpointNum = chkpNum;
	}
	else
	{
		granularity = ORIOLEDB_SEGMENT_SIZE;
	}

	while (amount > 0)
	{
		int			segno = curOffset / ORIOLEDB_SEGMENT_SIZE;
		int			partno = 0;
		File		file;
		uint32		loadId = 0;

		if (orioledb_s3_mode)
		{
			tag.segNum = segno;
			partno = (curOffset % ORIOLEDB_SEGMENT_SIZE) / ORIOLEDB_S3_PART_SIZE;
			s3_header_lock_part(tag, partno, &loadId);
		}

		file = btree_open_smgr_file(desc, segno, chkpNum, loadId);
		if ((curOffset + amount) / granularity == curOffset / granularity)
		{
			result += OFileWrite(file, buffer, amount,
								 curOffset % ORIOLEDB_SEGMENT_SIZE + (orioledb_s3_mode ? ORIOLEDB_BLCKSZ : 0),
								 WAIT_EVENT_DATA_FILE_WRITE);
			if (orioledb_s3_mode)
				s3_header_unlock_part(tag, partno, true);
			break;
		}
		else
		{
			int			stepAmount = granularity - curOffset % granularity;

			Assert(amount >= stepAmount);
			result += OFileWrite(file, buffer, stepAmount,
								 curOffset % ORIOLEDB_SEGMENT_SIZE + (orioledb_s3_mode ? ORIOLEDB_BLCKSZ : 0),
								 WAIT_EVENT_DATA_FILE_WRITE);
			buffer += stepAmount;
			curOffset += stepAmount;
			amount -= stepAmount;
		}

		if (orioledb_s3_mode)
			s3_header_unlock_part(tag, partno, true);
	}

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

	return result;
}

int
btree_smgr_read(BTreeDescr *desc, char *buffer, uint32 chkpNum,
				int amount, off_t offset)
{
	int			result = 0;
	off_t		granularity;
	S3HeaderTag tag = {0};

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

	if (orioledb_s3_mode)
	{
		granularity = ORIOLEDB_S3_PART_SIZE;
		tag.datoid = desc->oids.datoid;
		tag.relnode = desc->oids.relnode;
		tag.checkpointNum = chkpNum;
	}
	else
	{
		granularity = ORIOLEDB_SEGMENT_SIZE;
	}

	while (amount > 0)
	{
		int			segno = offset / ORIOLEDB_SEGMENT_SIZE;
		int			partno = 0;
		File		file;
		uint32		loadId = 0;

		if (orioledb_s3_mode)
		{
			tag.segNum = segno;
			partno = (offset % ORIOLEDB_SEGMENT_SIZE) / ORIOLEDB_S3_PART_SIZE;
			s3_header_lock_part(tag, partno, &loadId);
		}

		file = btree_open_smgr_file(desc, segno, chkpNum, loadId);
		if ((offset + amount) / granularity == offset / granularity)
		{
			result += OFileRead(file, buffer, amount,
								offset % ORIOLEDB_SEGMENT_SIZE + (orioledb_s3_mode ? ORIOLEDB_BLCKSZ : 0),
								WAIT_EVENT_DATA_FILE_READ);
			if (orioledb_s3_mode)
				s3_header_unlock_part(tag, partno, false);
			break;
		}
		else
		{
			int			stepAmount = granularity - offset % granularity;

			Assert(amount >= stepAmount);
			result += OFileRead(file, buffer, stepAmount,
								offset % ORIOLEDB_SEGMENT_SIZE + (orioledb_s3_mode ? ORIOLEDB_BLCKSZ : 0),
								WAIT_EVENT_DATA_FILE_READ);
			buffer += stepAmount;
			offset += stepAmount;
			amount -= stepAmount;
		}

		if (orioledb_s3_mode)
			s3_header_unlock_part(tag, partno, false);
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
		uint32		loadId = 0;

		if (orioledb_s3_mode)
		{
			S3HeaderTag tag = {.datoid = desc->oids.datoid,
				.relnode = desc->oids.relnode,
				.checkpointNum = chkpNum,
			.segNum = segno};

			loadId = s3_header_get_load_id(tag);
		}

		file = btree_open_smgr_file(desc, segno, chkpNum, loadId);
		if ((offset + amount) / ORIOLEDB_SEGMENT_SIZE == segno)
		{
			FileWriteback(file,
						  offset % ORIOLEDB_SEGMENT_SIZE + (orioledb_s3_mode ? ORIOLEDB_BLCKSZ : 0),
						  amount, WAIT_EVENT_DATA_FILE_FLUSH);
			break;
		}
		else
		{
			int			stepAmount = ORIOLEDB_SEGMENT_SIZE - offset % ORIOLEDB_SEGMENT_SIZE;

			Assert(amount >= stepAmount);
			FileWriteback(file,
						  offset % ORIOLEDB_SEGMENT_SIZE + (orioledb_s3_mode ? ORIOLEDB_BLCKSZ : 0),
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
		uint32		loadId = 0;

		if (orioledb_s3_mode)
		{
			S3HeaderTag tag = {.datoid = desc->oids.datoid,
				.relnode = desc->oids.relnode,
				.checkpointNum = chkpNum,
			.segNum = num};

			loadId = s3_header_get_load_id(tag);
		}

		file = btree_open_smgr_file(desc, num, chkpNum, loadId);
		FileSync(file, WAIT_EVENT_DATA_FILE_SYNC);
	}
}

void
btree_smgr_punch_hole(BTreeDescr *desc, uint32 chkpNum,
					  off_t offset, int length)
{
	Assert(!orioledb_s3_mode && !use_mmap && !use_device);

	while (length > 0)
	{
		File		file;
		int			fd;
		int			segno = offset / ORIOLEDB_SEGMENT_SIZE;
		off_t		segoffset;
		int			seglength;
		int			ret;

		file = btree_open_smgr_file(desc, segno, chkpNum, 0);
		fd = FileGetRawDesc(file);

		segoffset = offset % ORIOLEDB_SEGMENT_SIZE;
		if ((offset + length) / ORIOLEDB_SEGMENT_SIZE == segno)
		{
			seglength = length;
			length = 0;
		}
		else
		{
			seglength = ORIOLEDB_SEGMENT_SIZE - segoffset;
			Assert(length >= seglength);

			offset += seglength;
			length -= seglength;
		}
#ifdef __APPLE__
		{
			fpunchhole_t hole;

			memset(&hole, 0, sizeof(hole));
			hole.fp_offset = segoffset;
			hole.fp_length = seglength;
			ret = fcntl(fd, F_PUNCHHOLE, &hole);
		}
#else
		ret = fallocate(fd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE,
						segoffset, seglength);
#endif
		if (ret < 0)
		{
			int			save_errno = errno;

			ereport(WARNING,
					(errcode_for_file_access(),
					 errmsg("fail to punch sparse file hole datoid=%u relnode=%u offset=%llu length=%d (%d %s)",
							desc->oids.datoid, desc->oids.relnode,
							(unsigned long long) offset, length, save_errno, strerror(save_errno))));
		}
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

		if (orioledb_use_sparse_files)
		{
			try_to_punch_holes(desc);
			Assert(free_buf_num + 1 <= metaPage->punchHolesChkpNum);
		}

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
			if (old_tag.type == 'm')
			{
				uint32		chkpNum = o_get_latest_chkp_num(tag.datoid,
															tag.relnode,
															checkpoint_state->lastCheckpointNumber,
															NULL);

				if (old_tag.num < chkpNum)
					seq_buf_remove_file(&old_tag);
			}
			else
			{
				Assert(old_tag.type == 't');
				if (!orioledb_use_sparse_files ||
					old_tag.num <= metaPage->punchHolesChkpNum)
					seq_buf_remove_file(&old_tag);
			}
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
		int			threshold = ORIOLEDB_S3_PART_SIZE / (OCompressIsValid(desc->compress) ? ORIOLEDB_COMP_BLCKSZ : ORIOLEDB_BLCKSZ);
		BTreeMetaPage *metaPage = BTREE_GET_META(desc);

		extent->off = pg_atomic_fetch_add_u64(&metaPage->datafileLength[chkpNum % 2], len);
		extent->len = len;

		if ((extent->off + threshold - 1) / threshold !=
			(extent->off + threshold - 1 + len) / threshold)
		{
			Assert((extent->off + threshold - 1) / threshold + 1 ==
				   (extent->off + threshold - 1 + len) / threshold);
			s3_headers_increase_loaded_parts(1);
		}

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

	if ((desc->storageType == BTreeStoragePersistence || desc->storageType == BTreeStorageUnlogged) &&
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
		if (!err)
		{
			uint8		page_version;

			page_version = ((OrioleDBOndiskPageHeader *) img)->page_version;
			if (page_version != ORIOLEDB_PAGE_VERSION)
			{
				/*
				 * Now we have only one page version (1). When we have
				 * different versions we'll need to bump ORIOLEDB_PAGE_VERSION
				 * and add on-the-fly conversion function from all previous
				 * page versions in this place
				 */
				elog(FATAL, "Page version %u of OrioleDB cluster is not among supported for conversion %u", page_version, ORIOLEDB_PAGE_VERSION);
			}
		}
	}
	else
	{
		char		buf[ORIOLEDB_BLCKSZ];
		bool		compressed = len != (ORIOLEDB_BLCKSZ / ORIOLEDB_COMP_BLCKSZ);

		extent->off = offset;
		extent->len = len;

		if (orioledb_s3_mode)
		{
			chkpNum = S3_GET_CHKP_NUM(offset);
			offset &= S3_OFFSET_MASK;
		}

		if (compressed)
		{
			byte_offset = (off_t) offset * (off_t) ORIOLEDB_COMP_BLCKSZ;
			read_size = len * ORIOLEDB_COMP_BLCKSZ;

			err = btree_smgr_read(desc, buf, chkpNum, read_size, byte_offset) != read_size;

			if (!err)
			{
				OrioleDBOndiskPageHeader ondisk_page_header;

				memcpy(&ondisk_page_header, buf, sizeof(OrioleDBOndiskPageHeader));

				if (ondisk_page_header.page_version != ORIOLEDB_PAGE_VERSION)
				{
					/*
					 * Now we have only one page version (1). When we have
					 * different versions we'll need to bump
					 * ORIOLEDB_PAGE_VERSION and add on-the-fly conversion
					 * function from all previous page versions after
					 * decompression.
					 */
					elog(FATAL, "Page version %u of OrioleDB cluster is not among supported for conversion %u", ondisk_page_header.page_version, ORIOLEDB_PAGE_VERSION);
				}

				if (ondisk_page_header.compress_version != ORIOLEDB_COMPRESS_VERSION)
				{
					/*
					 * Now we have only one compress version (1). When we have
					 * different versions we'll need to bump
					 * ORIOLEDB_COMPRESS_VERSION and add on-the-fly conversion
					 * function from all previous compress versions
					 * before/during decompression.
					 */
					elog(FATAL, "Page version %u of OrioleDB cluster is not among supported for conversion %u", ondisk_page_header.page_version, ORIOLEDB_PAGE_VERSION);
				}

				o_decompress_page(buf + sizeof(OrioleDBOndiskPageHeader), ondisk_page_header.compress_page_size, img);
			}
		}
		else
		{
			OrioleDBOndiskPageHeader ondisk_page_header;

			byte_offset = (off_t) offset * (off_t) ORIOLEDB_COMP_BLCKSZ;
			read_size = sizeof(OrioleDBOndiskPageHeader);

			/* details about writed image parts are in write_page_to_disk */
			err = btree_smgr_read(desc, (Pointer) &ondisk_page_header, chkpNum, read_size, byte_offset) != read_size;
			byte_offset += read_size;

			if (!err)
			{
				size_t		skipped = offsetof(BTreePageHeader, undoLocation);
				BTreePageHeader *btree_page_header;

				memset(img, 0, skipped);
				read_size = ORIOLEDB_BLCKSZ - skipped;
				err = btree_smgr_read(desc, img + skipped, chkpNum, read_size, byte_offset) != read_size;

				if (!err)
				{
					if (ondisk_page_header.page_version != ORIOLEDB_PAGE_VERSION)
					{
						/*
						 * Now we have only one page version (1). When we have
						 * different versions we'll need to bump
						 * ORIOLEDB_PAGE_VERSION and add on-the-fly conversion
						 * function from all previous page versions here
						 */
						elog(FATAL, "Page version %u of OrioleDB cluster is not among supported for conversion %u", ondisk_page_header.page_version, ORIOLEDB_PAGE_VERSION);
					}
				}
				btree_page_header = (BTreePageHeader *) img;
				btree_page_header->o_header.checkpointNum = ondisk_page_header.checkpointNum;
			}
		}
	}

	return !err;
}

/*
 * Writes a page to the disk. An array of file offsets must be valid.
 */
static bool
write_page_to_disk(BTreeDescr *desc, FileExtent *extent, uint32 curChkpNum,
				   Pointer page, off_t page_size)
{

	off_t		byte_offset,
				write_size;
	bool		err = false;
	uint32		chkpNum = 0;
	char		buf[ORIOLEDB_BLCKSZ];

	Assert(sizeof(OrioleDBOndiskPageHeader) == O_PAGE_HEADER_SIZE);
	Assert(FileExtentOffIsValid(extent->off));
	if (!OCompressIsValid(desc->compress))
	{
		OrioleDBOndiskPageHeader *ondisk_page_header;

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

		memset(buf, 0, sizeof(OrioleDBOndiskPageHeader));
		ondisk_page_header = (OrioleDBOndiskPageHeader *) buf;
		ondisk_page_header->checkpointNum = curChkpNum;
		ondisk_page_header->page_version = ORIOLEDB_PAGE_VERSION;
		memcpy(&buf[sizeof(OrioleDBOndiskPageHeader)], page + sizeof(OrioleDBOndiskPageHeader), ORIOLEDB_BLCKSZ - sizeof(OrioleDBOndiskPageHeader));

		err = btree_smgr_write(desc, buf, chkpNum, write_size, byte_offset) != write_size;
	}
	else
	{
		OrioleDBOndiskPageHeader ondisk_page_header = {0};

		byte_offset = (off_t) extent->off;
		if (orioledb_s3_mode)
		{
			chkpNum = S3_GET_CHKP_NUM(byte_offset);
			byte_offset &= S3_OFFSET_MASK;
		}
		byte_offset *= (off_t) ORIOLEDB_COMP_BLCKSZ;

		/*
		 * overflow protection
		 */
		Assert(sizeof(((OrioleDBOndiskPageHeader *) 0)->compress_page_size) == sizeof(uint16));
		Assert(ORIOLEDB_BLCKSZ < UINT16_MAX);

		/* we need to write header first */
		ondisk_page_header.compress_page_size = page_size;
		ondisk_page_header.checkpointNum = curChkpNum;
		ondisk_page_header.compress_version = ORIOLEDB_COMPRESS_VERSION;
		ondisk_page_header.page_version = ORIOLEDB_PAGE_VERSION;

		write_size = sizeof(OrioleDBOndiskPageHeader);
		err = btree_smgr_write(desc, (char *) &ondisk_page_header, chkpNum, write_size, byte_offset) != write_size;
		byte_offset += write_size;

		if (err)
			return false;

		if (page_size != ORIOLEDB_BLCKSZ)
		{
			write_size = extent->len * ORIOLEDB_COMP_BLCKSZ - sizeof(OrioleDBOndiskPageHeader);
			err = btree_smgr_write(desc, page, chkpNum, write_size, byte_offset) != write_size;
		}
		else
		{
			size_t		skipped = offsetof(BTreePageHeader, undoLocation);

			/* Skipping chkpNum because it is present in BTreePageHeader */
			page += skipped;
			write_size = ORIOLEDB_BLCKSZ - skipped;
			err = btree_smgr_write(desc, page, chkpNum, write_size, byte_offset) != write_size;
		}
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
	OFixedKey	target_hikey;
	int			target_level;
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

	BTREE_PAGE_LOCATOR_NEXT(parent_page, parent_loc);
	if (BTREE_PAGE_LOCATOR_IS_VALID(parent_page, parent_loc))
		copy_fixed_page_key(desc, &target_hikey, parent_page, parent_loc);
	else if (!O_PAGE_IS(parent_page, RIGHTMOST))
		copy_fixed_hikey(desc, &target_hikey, parent_page);
	else
		clear_fixed_key(&target_hikey);
	target_level = PAGE_GET_LEVEL(parent_page) - 1;

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
						errmsg("could not read page with file offset " UINT64_FORMAT " from %s: %m",
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
		 * In S3 mode schedule load of all the page children for faster
		 * warmup.
		 */
		BTREE_PAGE_FOREACH_ITEMS(page, &loc)
		{
			BTreeNonLeafTuphdr *tupHdr;

			tupHdr = (BTreeNonLeafTuphdr *) BTREE_PAGE_LOCATOR_GET_ITEM(page, &loc);
			(void) s3_schedule_downlink_load(desc, tupHdr->downlink);
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
	if (PAGE_GET_LEVEL(page) != target_level)
		ereport(PANIC, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("error reading downlink %X/%X in relfile (%u, %u)",
							   (uint32) (downlink >> 32), (uint32) (downlink),
							   desc->oids.datoid, desc->oids.relnode),
						errdetail("Level mismatch, expected: %d, found: %d",
								  PAGE_GET_LEVEL(page), target_level)));

	if (O_PAGE_IS(page, RIGHTMOST))
	{
		OFindPageResult result PG_USED_FOR_ASSERTS_ONLY;

		if (!O_TUPLE_IS_NULL(target_hikey.tuple))
			ereport(PANIC, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
							errmsg("error reading downlink %X/%X in relfile (%u, %u)",
								   (uint32) (downlink >> 32), (uint32) (downlink),
								   desc->oids.datoid, desc->oids.relnode),
							errdetail("Hikeys don't match.")));
		result = refind_page(context, NULL, BTreeKeyRightmost,
							 PAGE_GET_LEVEL(page) + 1,
							 parent_blkno, parent_change_count);
		Assert(result == OFindPageResultSuccess);
	}
	else
	{
		OTuple		hikey;
		OFindPageResult result PG_USED_FOR_ASSERTS_ONLY;

		BTREE_PAGE_GET_HIKEY(hikey, page);

		if (O_TUPLE_IS_NULL(target_hikey.tuple) ||
			o_btree_cmp(desc, &hikey, BTreeKeyNonLeafKey, &target_hikey, BTreeKeyNonLeafKey) != 0)
			ereport(PANIC, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
							errmsg("error reading downlink %X/%X in relfile (%u, %u)",
								   (uint32) (downlink >> 32), (uint32) (downlink),
								   desc->oids.datoid, desc->oids.relnode),
							errdetail("Hikeys don't match.")));
		result = refind_page(context, &hikey, BTreeKeyPageHiKey,
							 PAGE_GET_LEVEL(page) + 1, parent_blkno,
							 parent_change_count);
		Assert(result == OFindPageResultSuccess);
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
		if (*size > (ORIOLEDB_BLCKSZ - ORIOLEDB_COMP_BLCKSZ - sizeof(OrioleDBOndiskPageHeader)))
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

	less_num = header->o_header.checkpointNum < checkpoint_number;
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
			header->o_header.checkpointNum = checkpoint_number;
			header = (BTreePageHeader *) page;
		}
		header->o_header.checkpointNum = checkpoint_number;
	}
	else
	{
		Assert(header->o_header.checkpointNum == checkpoint_number);
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
						errmsg("could not (re) allocate file blocks for page %d to file %s: %m",
							   blkno, btree_smgr_filename(desc, 0, checkpoint_number))));
	}

	Assert(FileExtentIsValid(page_desc->fileExtent));

	if (!write_page_to_disk(desc, &page_desc->fileExtent, checkpoint_number, write_img, write_size))
	{
		ereport(PANIC, (errcode_for_file_access(),
						errmsg("could not write page %d to file %s with offset %lu: %m",
							   blkno,
							   btree_smgr_filename(desc, page_desc->fileExtent.off, checkpoint_number),
							   (unsigned long) page_desc->fileExtent.off)));

		return InvalidDiskDownlink;
	}

	Assert(FileExtentIsValid(page_desc->fileExtent));
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
						errmsg("could not get free file offset for write page to file %s: %m",
							   btree_smgr_filename(desc, 0, 0))));

		return InvalidDiskDownlink;
	}

	Assert(FileExtentIsValid(*extent));

	if (!write_page_to_disk(desc, extent, chkpNum, write_img, write_size))
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
						errmsg("could not write autonomous page to file %s with offset %lu: %m",
							   btree_smgr_filename(desc, offset, chkpNum),
							   (unsigned long) offset)));

		return InvalidDiskDownlink;
	}

	Assert(FileExtentIsValid(*extent));
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
		chkpNum = checkpoint_state->lastCheckpointNumber;
	else
		chkpNum = 0;

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

	if (orioledb_s3_mode)
	{
		int			threshold = ORIOLEDB_S3_PART_SIZE / (OCompressIsValid(desc->compress) ? ORIOLEDB_COMP_BLCKSZ : ORIOLEDB_BLCKSZ);

		if ((extent->off + threshold - 1) / threshold !=
			(extent->off + threshold - 1 + extent->len) / threshold)
		{
			S3HeaderTag tag;
			uint64		offset = (extent->off + extent->len - 1) * (OCompressIsValid(desc->compress) ? ORIOLEDB_COMP_BLCKSZ : ORIOLEDB_BLCKSZ);
			int			index;

			Assert((extent->off + threshold - 1) / threshold + 1 ==
				   (extent->off + threshold - 1 + extent->len) / threshold);

			tag.datoid = desc->oids.datoid;
			tag.relnode = desc->oids.relnode;
			tag.checkpointNum = chkpNum;
			tag.segNum = offset / ORIOLEDB_SEGMENT_SIZE;
			index = (offset % ORIOLEDB_SEGMENT_SIZE) / ORIOLEDB_S3_PART_SIZE;
			s3_header_mark_part_loading(tag, index);
			s3_header_mark_part_loaded(tag, index);
			s3_headers_increase_loaded_parts(1);
		}

		extent->off |= (uint64) chkpNum << S3_CHKP_NUM_SHIFT;
	}

	Assert(FileExtentIsValid(*extent));

	if (!write_page_to_disk(desc, extent, 0, write_img, write_size))
	{
		ereport(PANIC, (errcode_for_file_access(),
						errmsg("could not write autonomous page to file %s with offset %lu: %m",
							   btree_smgr_filename(desc, extent[0].off, chkpNum),
							   (unsigned long) extent[0].off)));

		return InvalidDiskDownlink;
	}

	Assert(FileExtentIsValid(*extent));
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

			if (!try_lock_page(child))
				return false;

			/*
			 * It's worth less to write non-leaf page, if it's going to anyway
			 * become dirty after writing of child.
			 */
			if (IS_DIRTY(child) || desc->ionum >= 0)
			{
				unlock_page(child);
				return false;
			}

			/* XXX: should we also consider checkpoint number of child page? */
			Assert(FileExtentIsValid(desc->fileExtent));
			tuphdr->downlink = MAKE_ON_DISK_DOWNLINK(desc->fileExtent);
			unlock_page(child);
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
		Assert(FileExtentIsValid(page_desc->fileExtent));
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
								errmsg("could not evict page %d to disk: %m", blkno)));
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
			OFindPageResult result PG_USED_FOR_ASSERTS_ONLY;

			/* Refind parent */
			BTREE_PAGE_FIND_SET(context, DOWNLINK_LOCATION);
			if (O_PAGE_IS(p, RIGHTMOST))
			{
				result = refind_page(context, NULL, BTreeKeyRightmost,
									 PAGE_GET_LEVEL(p) + 1,
									 parent_blkno, parent_change_count);
			}
			else
			{
				OTuple		hikey;

				BTREE_PAGE_GET_HIKEY(hikey, p);
				result = refind_page(context, &hikey, BTreeKeyPageHiKey,
									 PAGE_GET_LEVEL(p) + 1,
									 parent_blkno, parent_change_count);
			}
			Assert(result == OFindPageResultSuccess);

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
								errmsg("could not evict page %d to disk: %m", blkno)));
			}
			else
			{
				if (dirty_parent)
					MARK_DIRTY(desc, parent_blkno);

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
btree_finalize_private_seq_bufs(BTreeDescr *desc, EvictedTreeData *evicted_data,
								bool notModified)
{
	int			chkp_index;
	bool		is_compressed = OCompressIsValid(desc->compress);

	Assert(desc->storageType == BTreeStorageTemporary ||
		   desc->storageType == BTreeStoragePersistence);

	/* we must not evict BTree under checkpoint */

	if (desc->storageType == BTreeStoragePersistence)
	{
		chkp_index = SEQ_BUF_SHARED_EXIST(desc->nextChkp[0].shared) ? 0 : 1;

		Assert(!SEQ_BUF_SHARED_EXIST(desc->nextChkp[1 - chkp_index].shared));
		Assert(!SEQ_BUF_SHARED_EXIST(desc->tmpBuf[1 - chkp_index].shared));
		Assert(is_compressed || SEQ_BUF_SHARED_EXIST(desc->freeBuf.shared));
		Assert(SEQ_BUF_SHARED_EXIST(desc->nextChkp[chkp_index].shared));
		Assert(SEQ_BUF_SHARED_EXIST(desc->tmpBuf[chkp_index].shared));
	}
	else
	{
		chkp_index = SEQ_BUF_SHARED_EXIST(desc->tmpBuf[0].shared) ? 0 : 1;

		Assert(!SEQ_BUF_SHARED_EXIST(desc->tmpBuf[1 - chkp_index].shared));
		Assert(is_compressed || SEQ_BUF_SHARED_EXIST(desc->freeBuf.shared));
		Assert(SEQ_BUF_SHARED_EXIST(desc->tmpBuf[chkp_index].shared));
	}

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

	if (desc->storageType == BTreeStoragePersistence)
	{
		evicted_data->nextChkp.tag = desc->nextChkp[chkp_index].shared->tag;
		if (notModified)
			seq_buf_close_file(&desc->nextChkp[chkp_index]);
		else
			evicted_data->nextChkp.offset = seq_buf_finalize(&desc->nextChkp[chkp_index]);
		FREE_PAGE_IF_VALID(desc->ppool, desc->nextChkp[chkp_index].shared->pages[0]);
		FREE_PAGE_IF_VALID(desc->ppool, desc->nextChkp[chkp_index].shared->pages[1]);

		evicted_data->tmpBuf.tag = desc->tmpBuf[chkp_index].shared->tag;
		if (notModified)
			seq_buf_close_file(&desc->nextChkp[chkp_index]);
		else
			evicted_data->tmpBuf.offset = seq_buf_finalize(&desc->tmpBuf[chkp_index]);
		FREE_PAGE_IF_VALID(desc->ppool, desc->tmpBuf[chkp_index].shared->pages[0]);
		FREE_PAGE_IF_VALID(desc->ppool, desc->tmpBuf[chkp_index].shared->pages[1]);
	}
	else
	{
		evicted_data->tmpBuf.tag = desc->tmpBuf[chkp_index].shared->tag;
		if (!notModified)
			evicted_data->tmpBuf.offset = seq_buf_finalize(&desc->tmpBuf[chkp_index]);
		FREE_PAGE_IF_VALID(desc->ppool, desc->tmpBuf[chkp_index].shared->pages[0]);
		FREE_PAGE_IF_VALID(desc->ppool, desc->tmpBuf[chkp_index].shared->pages[1]);
	}
}

/*
 * Evict the tree, assuming rootPageBlkno page is locked.
 */
static bool
evict_btree(BTreeDescr *desc, uint32 checkpoint_number)
{
	OInMemoryBlkno root_blkno = desc->rootInfo.rootPageBlkno;
	Page		rootPageBlkno = O_GET_IN_MEMORY_PAGE(root_blkno);
	OrioleDBPageDesc *root_desc = O_GET_IN_MEMORY_PAGEDESC(root_blkno);
	BTreeMetaPage *metaPage = BTREE_GET_META(desc);
	CheckpointFileHeader file_header = {0};
	EvictedTreeData evicted_tree_data = {{0}};
	uint64		new_downlink;
	char		img[ORIOLEDB_BLCKSZ];
	bool		was_dirty;
	int			i PG_USED_FOR_ASSERTS_ONLY;
	uint32		chkpNum = 0;
	bool		notModified;
	bool		hasMetaLock = LWLockHeldByMe(&checkpoint_state->oTablesMetaLock);

	Assert(ORootPageIsValid(desc) && OMetaPageIsValid(desc) &&
		   O_PAGE_STATE_IS_LOCKED(pg_atomic_read_u64(&(O_PAGE_HEADER(rootPageBlkno)->state))));

	/* we check it before */
	Assert(!RightLinkIsValid(BTREE_PAGE_GET_RIGHTLINK(rootPageBlkno)));
	if (orioledb_s3_mode)
	{
		btree_s3_flush(desc, checkpoint_number);
	}

	was_dirty = IS_DIRTY(root_blkno);

	/*
	 * Checking FileExtentIsValid() is essential for just created temporary
	 * trees which aren't dirty, but don't have fileExtent initialized.
	 */
	if (was_dirty || !FileExtentIsValid(root_desc->fileExtent))
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
		Assert(FileExtentIsValid(root_desc->fileExtent));
		new_downlink = MAKE_ON_DISK_DOWNLINK(root_desc->fileExtent);
		unlock_page(root_blkno);
	}

	if (!hasMetaLock)
	{
		if (!LWLockConditionalAcquire(&checkpoint_state->oTablesMetaLock,
									  LW_SHARED))
			return false;
	}

	file_header.rootDownlink = new_downlink;

	ppool_free_page(desc->ppool, root_blkno, NULL);

	if (orioledb_s3_mode)
		chkpNum = S3_GET_CHKP_NUM(DOWNLINK_GET_DISK_OFF(new_downlink));

	file_header.datafileLength = pg_atomic_read_u64(&metaPage->datafileLength[chkpNum % 2]);
	file_header.leafPagesNum = pg_atomic_read_u32(&metaPage->leafPagesNum);
	file_header.ctid = pg_atomic_read_u64(&metaPage->ctid);
	file_header.bridgeCtid = pg_atomic_read_u64(&metaPage->bridge_ctid);
	file_header.numFreeBlocks = pg_atomic_read_u64(&metaPage->numFreeBlocks);
#ifdef USE_ASSERT_CHECKING
	for (i = 0; i < NUM_SEQ_SCANS_ARRAY_SIZE; i++)
		Assert(pg_atomic_read_u32(&metaPage->numSeqScans[i]) == 0);
#endif

	evicted_tree_data.key.datoid = desc->oids.datoid;
	evicted_tree_data.key.relnode = desc->oids.relnode;
	evicted_tree_data.file_header = file_header;
	evicted_tree_data.maxLocation[0] = metaPage->partsInfo[0].writeMaxLocation;
	evicted_tree_data.maxLocation[1] = metaPage->partsInfo[1].writeMaxLocation;
	evicted_tree_data.dirtyFlag1 = metaPage->dirtyFlag1;
	evicted_tree_data.dirtyFlag2 = metaPage->dirtyFlag2;
	evicted_tree_data.punchHolesChkpNum = metaPage->punchHolesChkpNum;

	notModified = (!metaPage->dirtyFlag1 && !metaPage->dirtyFlag2);

	/*
	 * Free all private seq buf pages and get their offsets
	 */
	if (!orioledb_s3_mode || desc->storageType == BTreeStorageTemporary)
		btree_finalize_private_seq_bufs(desc, &evicted_tree_data, notModified);

	ppool_free_page(desc->ppool, desc->rootInfo.metaPageBlkno, NULL);

	desc->rootInfo.rootPageBlkno = OInvalidInMemoryBlkno;
	desc->rootInfo.metaPageBlkno = OInvalidInMemoryBlkno;

	perform_writeback(&io_writeback);

	/*
	 * Check if we can skip the evicted data if tree has no modification after
	 * writing the last *.map file.
	 */
	if (desc->storageType != BTreeStoragePersistence || !notModified)
		insert_evicted_data(&evicted_tree_data);

	/*
	 * Shared descr drops to signalize other backends that tree is evicted.
	 * Backends and workers can create a new SharedRootInfo* after this.
	 */
	o_drop_shared_root_info(desc->oids.datoid, desc->oids.relnode);

	if (!hasMetaLock)
		LWLockRelease(&checkpoint_state->oTablesMetaLock);

	return true;
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

typedef struct
{
	bool		indexRegularLock;
	bool		indexCheckpointerLock;
	bool		tableRegularLock;
	bool		tableCheckpointerLock;
	ORelOids	tableOids;
} EvictBtreeLocksState;

/*
 * Acquire all the locks required to completely evict the tree.  We need to
 * take both regular and checkpointer locks.  Also, for PK we need to lock
 * the table as well, because a concurrent seq scan can lock only the table.
 */
static BTreeDescr *
get_evict_btree_locks(OInMemoryBlkno blkno, ORelOids oids, OIndexType type,
					  EvictBtreeLocksState *state)
{
	BTreeDescr *desc;
	OIndexDescr *id;
	bool		recovery = is_recovery_in_progress();
	bool		nested = false;

	if (!recovery && !(state->indexRegularLock = o_tables_rel_try_lock_extended(&oids, AccessExclusiveLock, &nested, false)))
		return NULL;

	if (nested)
		return NULL;

	if (!(state->indexCheckpointerLock = o_tables_rel_try_lock_extended(&oids, AccessExclusiveLock, &nested, true)))
		return NULL;

	if (nested)
		return NULL;

	desc = index_oids_get_btree_descr(oids, type);

	if (desc == NULL ||
		desc->rootInfo.rootPageBlkno != blkno)
		return NULL;

	if (desc->type != oIndexPrimary)
		return desc;

	id = (OIndexDescr *) desc->arg;
	state->tableOids = id->tableOids;

	if (!recovery && !(state->tableRegularLock = o_tables_rel_try_lock_extended(&state->tableOids, AccessExclusiveLock, &nested, false)))
		return NULL;

	if (nested)
		return NULL;

	if (!(state->tableCheckpointerLock = o_tables_rel_try_lock_extended(&state->tableOids, AccessExclusiveLock, &nested, true)))
		return NULL;

	if (nested)
		return NULL;

	desc = index_oids_get_btree_descr(oids, type);

	if (desc == NULL ||
		desc->rootInfo.rootPageBlkno != blkno)
		return NULL;

	return desc;
}

static void
release_evict_btree_locks(ORelOids oids, EvictBtreeLocksState *state)
{
	if (state->indexRegularLock)
		o_tables_rel_unlock_extended(&oids, AccessExclusiveLock, false);
	if (state->indexCheckpointerLock)
		o_tables_rel_unlock_extended(&oids, AccessExclusiveLock, true);
	if (state->tableRegularLock)
		o_tables_rel_unlock_extended(&state->tableOids, AccessExclusiveLock, false);
	if (state->tableCheckpointerLock)
		o_tables_rel_unlock_extended(&state->tableOids, AccessExclusiveLock, true);
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
				merge_tried = false;
	OFindPageResult findResult;
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

	/* Important to access the shared memory once */
	oids = *((volatile ORelOids *) &page_desc->oids);

	/*
	 * index_oids_get_btree_descr() might imply page eviction.  We shouldn't
	 * do this while holding a page lock.  So, we need to do this before
	 * locking the page.
	 */
	if (IS_SYS_TREE_OIDS(oids))
	{
		if (sys_tree_get_storage_type(oids.relnode) != BTreeStorageInMemory)
			desc = get_sys_tree(oids.relnode);
		else
			return OWalkPageSkipped;
	}
	else
	{
		/* Check is this index is visible for us */
		desc = index_oids_get_btree_descr(oids, page_desc->type);

		if (desc == NULL)
			return OWalkPageSkipped;
	}

	if (!try_lock_page(blkno))
		return OWalkPageSkipped;

	/* page is locked once we get here */

	if (!ORelOidsIsValid(page_desc->oids) ||
		page_desc->type == oIndexInvalid ||
		!ORelOidsIsEqual(oids, page_desc->oids))
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
							   | BTREE_PAGE_FIND_NO_FIX_SPLIT);
		if (O_PAGE_IS(p, RIGHTMOST))
		{
			findResult = find_page(&context, NULL, BTreeKeyRightmost, PAGE_GET_LEVEL(p) + 1);
		}
		else
		{
			OTuple		hikey;

			BTREE_PAGE_GET_HIKEY(hikey, p);
			findResult = find_page(&context, &hikey, BTreeKeyPageHiKey, PAGE_GET_LEVEL(p) + 1);
		}

		if (findResult != OFindPageResultSuccess)
		{
			Assert(findResult == OFindPageResultFailure);
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
		{
			unlock_page(context.items[context.index].blkno);
		}
		return OWalkPageSkipped;
	}

	if (evict && is_root)
	{
		EvictBtreeLocksState locksState;
		bool		result = false;

		if (tree_is_under_checkpoint(desc))
		{
			unlock_page(blkno);
			return OWalkPageSkipped;
		}

		memset(&locksState, 0, sizeof(locksState));

		desc = get_evict_btree_locks(blkno, oids, page_desc->type, &locksState);

		if (desc)
		{
			result = evict_btree(desc, checkpoint_number);
			o_invalidate_oids(oids);
		}
		else
		{
			unlock_page(blkno);
		}

		release_evict_btree_locks(oids, &locksState);

		return result ? OWalkPageEvicted : OWalkPageSkipped;
	}

	STOPEVENT(STOPEVENT_BEFORE_WRITE_PAGE, NULL);

	write_page(&context, blkno, img, checkpoint_number, evict, copy_blkno);

	STOPEVENT(STOPEVENT_AFTER_WRITE_PAGE, NULL);

	return evict ? OWalkPageEvicted : OWalkPageWritten;
}

static bool
write_tree_pages_recursive(UndoLogType undoType,
						   OInMemoryBlkno blkno, uint32 loadId,
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
	if (O_PAGE_GET_CHANGE_COUNT(p) != loadId)
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
		(void) write_tree_pages_recursive(undoType,
										  childPageNumbers[i],
										  childPageChangeCounts[i],
										  maxLevel,
										  evict);

	if (level <= maxLevel)
	{
		while (true)
		{
			reserve_undo_size(GET_PAGE_LEVEL_UNDO_TYPE(undoType),
							  2 * O_MERGE_UNDO_IMAGE_SIZE);
			if (walk_page(blkno, evict) != OWalkPageMerged)
				break;
		}
		release_undo_size(GET_PAGE_LEVEL_UNDO_TYPE(undoType));
	}

	return true;
}

static void
write_tree_pages(BTreeDescr *desc, int maxLevel, bool evict)
{
	o_btree_load_shmem(desc);
	if (!write_tree_pages_recursive(desc->undoType,
									desc->rootInfo.rootPageBlkno,
									desc->rootInfo.rootPageChangeCount,
									maxLevel, evict))
	{
		desc->rootInfo.rootPageBlkno = OInvalidInMemoryBlkno;
		desc->rootInfo.metaPageBlkno = OInvalidInMemoryBlkno;
		desc->rootInfo.rootPageChangeCount = 0;
		o_btree_load_shmem(desc);
		(void) write_tree_pages_recursive(desc->undoType,
										  desc->rootInfo.rootPageBlkno,
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
	bool		first_file_deleted = false;
	char	   *db_prefix;

	o_get_prefixes_for_relnode(datoid, relnode, NULL, &db_prefix);

	dir = opendir(db_prefix);

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
			 (!strcmp(file_ext, "tmp") || !strcmp(file_ext, "map") ||
			  !strcmp(file_ext, "evt")) &&
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
					filename = psprintf("%s/%u", db_prefix, relnode);
					callback(filename, 0, NULL, arg);
					pfree(filename);
					first_file_deleted = true;
				}

				if (file_segno != 0 || file_ext_p != NULL)
				{
					filename = psprintf("%s/%s", db_prefix, file->d_name);
					callback(filename, file_segno, file_ext_p, arg);
					pfree(filename);
				}
			}
		}
	}

	closedir(dir);
	pfree(db_prefix);
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
	bool		fsync = *(bool *) arg;

	if (segno == 0 && ext == NULL && fsync)
		durable_unlink(filename, ERROR);
	else
		unlink(filename);
}

bool
cleanup_btree_files(Oid datoid, Oid relnode, bool fsync)
{
	return iterate_relnode_files(datoid, relnode, unlink_callback, (void *) &fsync);
}

static void
fsync_callback(const char *filename, uint32 segno, char *ext, void *arg)
{
	if (ext == NULL || strcmp(ext, "tmp") != 0)
		fsync_fname(filename, false);
}

bool
fsync_btree_files(Oid datoid, Oid relnode)
{
	char	   *prefix;
	char	   *db_prefix;

	o_get_prefixes_for_relnode(datoid, relnode,
							   &prefix, &db_prefix);
	o_verify_dir_exists_or_create(prefix, NULL, NULL);
	o_verify_dir_exists_or_create(db_prefix, NULL, NULL);
	pfree(db_prefix);

	return iterate_relnode_files(datoid, relnode, fsync_callback, NULL);
}

void
try_to_punch_holes(BTreeDescr *desc)
{
	BTreeMetaPage *metaPage;
	File		file;
	uint64		file_size;
	char	   *filename,
				buf[ORIOLEDB_BLCKSZ];
	uint64		len = 0,
				i,
				buf_len;
	uint32		chkp_num;
	LWLock	   *metaLock;
	LWLock	   *punchHolesLock;

	Assert(orioledb_use_sparse_files);
	Assert(!OCompressIsValid(desc->compress));

	o_btree_load_shmem(desc);
	metaPage = BTREE_GET_META(desc);
	metaLock = &metaPage->metaLock;
	punchHolesLock = &metaPage->punchHolesLock;

	chkp_num = metaPage->punchHolesChkpNum + 1;
	while (can_use_checkpoint_extents(desc, chkp_num))
	{
		SeqBufTag	tag;
		bool		removeFile = false;

		LWLockAcquire(punchHolesLock, LW_EXCLUSIVE);

		if (chkp_num == metaPage->punchHolesChkpNum + 1)
		{
			if (chkp_num < metaPage->freeBuf.tag.num)
				removeFile = true;
		}
		else
		{
			chkp_num = metaPage->punchHolesChkpNum + 1;
			/* Try for next checkpoint number */
			LWLockRelease(punchHolesLock);
			continue;
		}

		tag.datoid = desc->oids.datoid;
		tag.relnode = desc->oids.relnode;
		tag.type = 't';
		tag.num = chkp_num;
		if (!seq_buf_file_exist(&tag))
		{
			/* table may be deleted or *.tmp file not created */
			LWLockAcquire(metaLock, LW_EXCLUSIVE);
			Assert(chkp_num == metaPage->punchHolesChkpNum + 1);
			metaPage->punchHolesChkpNum = chkp_num;
			LWLockRelease(metaLock);
			LWLockRelease(punchHolesLock);
			chkp_num++;
			continue;
		}

		/* free extents from *.tmp file */
		filename = get_seq_buf_filename(&tag);
		file = PathNameOpenFile(filename, O_RDONLY | PG_BINARY);
		if (file < 0)
			ereport(FATAL, (errcode_for_file_access(),
							errmsg("could not open file %s: %m", filename)));
		file_size = FileSize(file);

		while (true)
		{
			BlockNumber *cur_off;

			buf_len = OFileRead(file, buf, ORIOLEDB_BLCKSZ, len, WAIT_EVENT_DATA_FILE_READ);
			if (buf_len <= 0)
				break;

			cur_off = (BlockNumber *) buf;
			for (i = 0; i < buf_len; i += sizeof(BlockNumber))
			{
				btree_smgr_punch_hole(desc, chkp_num,
									  (off_t) (*cur_off) * (off_t) ORIOLEDB_BLCKSZ,
									  ORIOLEDB_BLCKSZ);
				cur_off++;
			}
			len += buf_len;
		}
		if (file_size != len)
			ereport(FATAL, (errcode_for_file_access(),
							errmsg("could not read data from checkpoint tmp file: %s %lu %lu: %m",
								   filename, len, file_size)));

		pfree(filename);
		FileClose(file);

		if (removeFile)
			seq_buf_remove_file(&tag);

		LWLockAcquire(metaLock, LW_EXCLUSIVE);
		Assert(chkp_num == metaPage->punchHolesChkpNum + 1);
		metaPage->punchHolesChkpNum = chkp_num;
		LWLockRelease(metaLock);

		LWLockRelease(punchHolesLock);

		/* Try for next checkpoint number */
		chkp_num++;
	}
}
