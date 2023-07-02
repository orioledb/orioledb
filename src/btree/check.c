/*-------------------------------------------------------------------------
 *
 * check.c
 *		Routines for checking OrioleDB B-tree.
 *
 * Copyright (c) 2021-2023 Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/btree/check.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "orioledb.h"

#include "btree/check.h"
#include "btree/io.h"
#include "btree/page_chunks.h"
#include "catalog/free_extents.h"
#include "checkpoint/checkpoint.h"
#include "recovery/recovery.h"
#include "tableam/descr.h"
#include "utils/compress.h"
#include "utils/page_pool.h"
#include "utils/seq_buf.h"
#include "utils/ucm.h"

#include "pgstat.h"
#include "access/transam.h"

/*
 * Dynamic array of file extents.
 */
typedef struct
{
	/* array of extents */
	FileExtent *extents;
	/* number of allocated extents */
	uint64		allocated;
	/* number of valid extents in the array */
	uint64		size;
	/* number of blocks in file containing by extents in the array */
	uint64		blocksCount;
} ExtentsArray;

typedef struct
{
	ExtentsArray busy;
	BTreeDescr *desc;
	bool		hasError;
	OBTreeFindPageContext context;
} BTreeCheckStatus;

static int	file_extent_cmp(const void *p1, const void *p2);
static void check_walk_btree(BTreeCheckStatus *status, OInMemoryBlkno blkno,
							 OInMemoryBlkno parentPagenum);
static void add_extent(ExtentsArray *arr, FileExtent extent);
static bool check_extents(ExtentsArray *busy, ExtentsArray *free);
static void get_free_extents(BTreeDescr *desc, ExtentsArray *free_extents,
							 bool force_file_check, uint32 chkp_num);
static void get_free_extents_from_file(SeqBufTag *tag, off_t offset,
									   ExtentsArray *free_extents, bool compressed);
static bool is_sorted_by_off(ExtentsArray *array);
static bool is_sorted_by_len_off(ExtentsArray *array);

bool
check_btree(BTreeDescr *desc, bool force_file_check)
{
	BTreeMetaPage *metaPageBlkno = BTREE_GET_META(desc);
	BTreeCheckStatus status;
	ExtentsArray free_extents;
	uint64		data_file_len = pg_atomic_read_u64(&metaPageBlkno->datafileLength[0]);	/* Fix for S3 mode */
	bool		is_compressed = OCompressIsValid(desc->compress);
	uint32		checkpoint_number = 0;
	bool		result,
				copy_blkno;

	memset(&status, 0, sizeof(BTreeCheckStatus));
	memset(&free_extents, 0, sizeof(ExtentsArray));

	/* get busy file extents */
	status.desc = desc;
	status.hasError = false;
	init_page_find_context(&status.context, desc, COMMITSEQNO_INPROGRESS, BTREE_PAGE_FIND_MODIFY);

	result = get_checkpoint_number(desc, desc->rootInfo.rootPageBlkno,
								   &checkpoint_number, &copy_blkno);
	if (!result)
	{
		elog(NOTICE, "Tree is under checkpoint now");
		return false;
	}

	Assert(checkpoint_number > 0);

	check_walk_btree(&status, desc->rootInfo.rootPageBlkno, OInvalidInMemoryBlkno);

	if (status.hasError)
		return false;

	if (desc->storageType != BTreeStoragePersistence)
		return true;

	/* get free file extents */
	get_free_extents(desc, &free_extents, force_file_check,
					 checkpoint_number - 1);

	if (status.hasError)
		return false;

	/* check extents */
	status.hasError = !check_extents(&status.busy, &free_extents);

	if (status.hasError)
		return false;

	if (data_file_len > status.busy.blocksCount + free_extents.blocksCount)
	{
		elog(NOTICE, "Not used file blocks from %lu to %lu",
			 status.busy.blocksCount + free_extents.blocksCount,
			 data_file_len);
		status.hasError = true;
	}
	else if (data_file_len < status.busy.blocksCount + free_extents.blocksCount)
	{
		elog(NOTICE, "Excess file blocks from %lu to %lu",
			 data_file_len,
			 status.busy.blocksCount + free_extents.blocksCount);
		status.hasError = true;
	}

	/* frees allocated bytes */
	if (status.busy.size > 0)
	{
		Assert(status.busy.extents != NULL);
		pfree(status.busy.extents);
	}

	if (free_extents.size > 0)
	{
		Assert(free_extents.extents != NULL);
		pfree(free_extents.extents);
	}

	if (checkpoint_number > 1)
	{
		/* file extents sort check */
		SeqBufTag	tag;
		ExtentsArray map_extents,
					tmp_extents;

		memset(&map_extents, 0, sizeof(ExtentsArray));
		memset(&tmp_extents, 0, sizeof(ExtentsArray));

		tag.datoid = desc->oids.datoid;
		tag.relnode = desc->oids.relnode;
		tag.num = checkpoint_number - 1;
		tag.type = 'm';


		if (seq_buf_file_exist(&tag))
		{
			get_free_extents_from_file(&tag, sizeof(CheckpointFileHeader),
									   &map_extents, is_compressed);
		}
		else
		{
			elog(NOTICE, "%s not exist", get_seq_buf_filename(&tag));
			status.hasError = true;
		}

		tag.type = 't';
		if (!is_compressed && seq_buf_file_exist(&tag))
		{
			get_free_extents_from_file(&tag, 0, &tmp_extents, false);
		}

		if (map_extents.size != 0)
		{
			bool		sorted = is_compressed ? is_sorted_by_len_off(&map_extents)
				: is_sorted_by_off(&map_extents);

			if (!sorted)
			{
				tag.type = 'm';
				elog(NOTICE, "%s file is not sorted", get_seq_buf_filename(&tag));
				status.hasError = true;
			}
			pfree(map_extents.extents);
		}

		if (tmp_extents.size != 0)
		{
			bool		sorted = is_compressed ? is_sorted_by_len_off(&tmp_extents)
				: is_sorted_by_off(&tmp_extents);

			if (!sorted)
			{
				tag.type = 't';
				elog(NOTICE, "%s file is not sorted", get_seq_buf_filename(&tag));
				status.hasError = true;
			}
			pfree(tmp_extents.extents);
		}
	}

	return !status.hasError;
}

/*
 * Appends extent into the extents array.
 */
static void
foreach_extent_append(BTreeDescr *desc, FileExtent extent, void *arg)
{
	ExtentsArray *arr = (ExtentsArray *) arg;

	add_extent(arr, extent);
}

/*
 * Gets free file extents for an index.
 */
static void
get_free_extents(BTreeDescr *desc, ExtentsArray *free_extents,
				 bool force_file_check, uint32 chkp_num)
{
	SeqBufTag	chkp_tag;
	bool		is_compressed = OCompressIsValid(desc->compress);

	chkp_tag.datoid = desc->oids.datoid;
	chkp_tag.relnode = desc->oids.relnode;

	if (force_file_check)
	{
		/*
		 * Reads free blocks from map file.
		 */
		chkp_tag.type = 'm';
		chkp_tag.num = chkp_num;

		get_free_extents_from_file(&chkp_tag, sizeof(CheckpointFileHeader),
								   free_extents, is_compressed);
	}
	else if (!is_compressed)
	{
		/*
		 * Reads free blocks as normal process for uncompressed index.
		 */
		off_t		freebuf_offset;
		uint32		num;

		chkp_tag = desc->freeBuf.shared->tag;
		freebuf_offset = seq_buf_get_offset(&desc->freeBuf);

		get_free_extents_from_file(&chkp_tag, freebuf_offset, free_extents, false);
		for (num = chkp_tag.num; num < chkp_num; num++)
		{
			chkp_tag.num = num + 1;
			chkp_tag.type = 't';
			get_free_extents_from_file(&chkp_tag, 0, free_extents, false);
		}
	}
	else
	{
		/*
		 * Reads free blocks as normal process for compressed index.
		 */
		foreach_free_extent(desc, foreach_extent_append, (void *) free_extents);
		chkp_tag.num = chkp_num + 1;
		chkp_tag.type = 't';
		get_free_extents_from_file(&chkp_tag, sizeof(CheckpointFileHeader),
								   free_extents, true);
	}
}

/*
 * Appends file extents from file to the free extents array.
 */
static void
get_free_extents_from_file(SeqBufTag *tag, off_t offset,
						   ExtentsArray *free_extents, bool compressed)
{
	char		buf[ORIOLEDB_BLCKSZ],
			   *filename;
	File		file;
	FileExtent	extent;
	off_t		bytes_read;
	uint32		off;
	int			i;

	filename = get_seq_buf_filename(tag);
	file = PathNameOpenFile(filename, O_RDONLY | PG_BINARY);
	if (file == -1)
	{
		ereport(NOTICE, (errcode_for_file_access(),
						 errmsg("could not open map file %s.", filename)));
		return;
	}

	do
	{
		bytes_read = OFileRead(file, buf, ORIOLEDB_BLCKSZ, offset,
							   WAIT_EVENT_DATA_FILE_READ);
		offset += bytes_read;

		i = 0;
		while (i < bytes_read)
		{
			if (compressed || use_device)
			{
				memcpy(&extent, buf + i, sizeof(FileExtent));
				i += sizeof(FileExtent);
			}
			else
			{
				memcpy(&off, buf + i, sizeof(uint32));
				i += sizeof(uint32);
				extent.off = off;
				extent.len = 1;
			}
			add_extent(free_extents, extent);
		}
	} while (bytes_read == ORIOLEDB_BLCKSZ);

	FileClose(file);
	pfree(filename);
}

/*
 * Returns true if the busy and free extents array do not intersect and have no
 * holes.
 */
static bool
check_extents(ExtentsArray *busy, ExtentsArray *free)
{
	FileExtent	cur;
	uint64		b,
				f,
				next_off;
	bool		result = true;

	qsort(busy->extents, busy->size, sizeof(FileExtent), file_extent_cmp);
	qsort(free->extents, free->size, sizeof(FileExtent), file_extent_cmp);

	b = 0;
	f = 0;
	cur.off = 0;
	cur.len = 0;
	while (true)
	{
		next_off = cur.off + cur.len;

		while (b < busy->size && next_off > busy->extents[b].off)
		{
			elog(NOTICE, "Excess busy extent %lu %u",
				 (unsigned long) busy->extents[b].off,
				 (unsigned) busy->extents[b].len);
			result = false;
			b++;
		}

		while (f < free->size && next_off > free->extents[f].off)
		{
			elog(NOTICE, "Excess free extent %lu %u",
				 (unsigned long) free->extents[f].off,
				 (unsigned) free->extents[f].len);
			result = false;
			f++;
		}

		if (f >= free->size && b >= busy->size)
			break;

		if (f >= free->size || (b < busy->size && file_extent_cmp(&free->extents[f], &busy->extents[b]) > 0))
		{
			if (next_off != busy->extents[b].off)
			{
				elog(NOTICE, "Extent %lu %u is neither free or busy",
					 (unsigned long) (next_off),
					 (unsigned) (busy->extents[b].off - next_off));
				result = false;
			}
			cur = busy->extents[b++];
		}
		else
		{
			if (next_off != free->extents[f].off)
			{
				elog(NOTICE, "Extent %lu %u is neither free or busy",
					 (unsigned long) (next_off),
					 (unsigned) (free->extents[f].off - next_off));
				result = false;
			}
			cur = free->extents[f++];
		}
	}

	return result;
}

/*
 * (off, len) sort comparator
 */
static int
file_extent_cmp(const void *p1, const void *p2)
{
	FileExtent	v1 = *((const FileExtent *) p1);
	FileExtent	v2 = *((const FileExtent *) p2);

	if (v1.off != v2.off)
		return v1.off > v2.off ? 1 : -1;
	if (v1.len != v2.len)
		return v1.len > v2.len ? 1 : -1;
	return 0;
}

/*
 * Returns false if the array is sorted by off order.
 */
static bool
is_sorted_by_off(ExtentsArray *array)
{
	uint64		i;
	bool		sorted = true;

	if (array->size > 1)
	{
		for (i = 1; i < array->size && sorted; i++)
		{
			sorted = array->extents[i - 1].off <= array->extents[i].off;
		}

		if (!sorted)
		{
			i--;
			elog(NOTICE, "Extents (%lu, %u), (%lu, %u) have wrong sort order",
				 (unsigned long) array->extents[i - 1].off,
				 (unsigned) array->extents[i - 1].len,
				 (unsigned long) array->extents[i].off,
				 (unsigned) array->extents[i].len);
		}
	}

	return sorted;
}

/*
 * Returns true if the array is sorted by (reverse len, off) order.
 */
static bool
is_sorted_by_len_off(ExtentsArray *array)
{
	uint64		i;
	int			cmp;
	bool		sorted = true;

	if (array->size > 1)
	{
		for (i = 1; i < array->size && sorted; i++)
		{
			cmp = array->extents[i - 1].len - array->extents[i].len;
			sorted = cmp > 0 || (cmp == 0 && array->extents[i - 1].off <= array->extents[i].off);
		}

		if (!sorted)
		{
			i--;
			elog(NOTICE, "Extents (%lu, %u), (%lu, %u) have wrong sort order",
				 (unsigned long) array->extents[i - 1].off,
				 (unsigned) array->extents[i - 1].len,
				 (unsigned long) array->extents[i].off,
				 (unsigned) array->extents[i].len);
		}
	}

	return sorted;
}

/*
 * Appends the extent to the array.
 */
static void
add_extent(ExtentsArray *arr, FileExtent extent)
{
	if (arr->size >= arr->allocated)
	{
		if (arr->allocated == 0)
		{
			arr->allocated = 16;
			arr->extents = (FileExtent *) palloc(sizeof(FileExtent) * arr->allocated);
		}
		else
		{
			arr->allocated *= 2;
			arr->extents = (FileExtent *) repalloc(arr->extents,
												   sizeof(FileExtent) * arr->allocated);
		}
	}
	arr->extents[arr->size++] = extent;
	arr->blocksCount += extent.len;
}

static void
check_walk_btree(BTreeCheckStatus *status, OInMemoryBlkno blkno,
				 OInMemoryBlkno parentPagenum)
{
	Page		p = O_GET_IN_MEMORY_PAGE(blkno);
	OrioleDBPageDesc *page_desc = O_GET_IN_MEMORY_PAGEDESC(blkno);
	OBTreeFindPageContext *context = &status->context;
	FileExtent	extent;

	Assert(OInMemoryBlknoIsValid(blkno));

	lock_page(blkno);
	if (OInMemoryBlknoIsValid(parentPagenum))
		unlock_page(parentPagenum);

	context->index++;
	context->items[context->index].blkno = blkno;
	context->items[context->index].pageChangeCount = O_PAGE_GET_CHANGE_COUNT(p);

	if (O_PAGE_IS(p, BROKEN_SPLIT))
	{
		elog(NOTICE, "BTree has a broken split.");
		status->hasError = true;
	}

	if (!O_PAGE_IS(p, LEAF))
	{
		BTreePageItemLocator loc;

		BTREE_PAGE_LOCATOR_FIRST(p, &loc);
		while (BTREE_PAGE_LOCATOR_IS_VALID(p, &loc))
		{
			Pointer		ptr = BTREE_PAGE_LOCATOR_GET_ITEM(p, &loc);
			BTreeNonLeafTuphdr *tuphdr = (BTreeNonLeafTuphdr *) ptr;

			if (DOWNLINK_IS_IN_MEMORY(tuphdr->downlink))
			{
				check_walk_btree(status, DOWNLINK_GET_IN_MEMORY_BLKNO(tuphdr->downlink),
								 blkno);
			}
			else if (DOWNLINK_IS_IN_IO(tuphdr->downlink))
			{
				wait_for_io_completion(DOWNLINK_GET_IO_LOCKNUM(tuphdr->downlink));
				continue;
			}
			else if (DOWNLINK_IS_ON_DISK(tuphdr->downlink))
			{
				context->items[context->index].locator = loc;
				load_page(context);
				continue;
			}
			BTREE_PAGE_LOCATOR_NEXT(p, &loc);
		}
	}

	if (FileExtentIsValid(page_desc->fileExtent))
	{
		extent = page_desc->fileExtent;
		add_extent(&status->busy, extent);
	}

	if (OInMemoryBlknoIsValid(parentPagenum))
		lock_page(parentPagenum);
	unlock_page(blkno);
	context->index--;
}

static void
btree_check_compression_recursive(BTreeDescr *desc, BTreeCompressStats *stats, OCompress lvl,
								  OBTreeFindPageContext *context, OInMemoryBlkno blkno)
{
	char		buf[ORIOLEDB_BLCKSZ];
	Page		p = O_GET_IN_MEMORY_PAGE(blkno);
	OrioleDBPageHeader *header = (OrioleDBPageHeader *) p;
	size_t		compressed_size;

	page_inc_usage_count(&desc->ppool->ucm, blkno,
						 pg_atomic_read_u32(&header->usageCount), false);

	context->index++;
	context->items[context->index].blkno = blkno;
	context->items[context->index].pageChangeCount = O_PAGE_GET_CHANGE_COUNT(p);

	if (!O_PAGE_IS(p, LEAF))
	{
		BTreePageItemLocator loc;

		BTREE_PAGE_LOCATOR_FIRST(p, &loc);
		while (BTREE_PAGE_LOCATOR_IS_VALID(p, &loc))
		{
			Pointer		ptr = BTREE_PAGE_LOCATOR_GET_ITEM(p, &loc);
			BTreeNonLeafTuphdr *tuphdr = (BTreeNonLeafTuphdr *) ptr;

			if (DOWNLINK_IS_IN_MEMORY(tuphdr->downlink))
			{
				btree_check_compression_recursive(desc, stats, lvl, context,
												  DOWNLINK_GET_IN_MEMORY_BLKNO(tuphdr->downlink));
			}
			else if (DOWNLINK_IS_IN_IO(tuphdr->downlink))
			{
				wait_for_io_completion(DOWNLINK_GET_IO_LOCKNUM(tuphdr->downlink));
				continue;
			}
			else if (DOWNLINK_IS_ON_DISK(tuphdr->downlink))
			{
				context->items[context->index].locator = loc;
				lock_page(blkno);
				load_page(context);
				unlock_page(blkno);
				continue;
			}
			BTREE_PAGE_LOCATOR_NEXT(p, &loc);
		}
	}

	memcpy(buf, p, ORIOLEDB_BLCKSZ);
	null_unused_bytes(buf);

	PG_TRY();
	{
		o_compress_page(buf, &compressed_size, lvl);

		stats->totalSize += ORIOLEDB_BLCKSZ;
		stats->totalCompressedSize += compressed_size;

		if (compressed_size > ORIOLEDB_BLCKSZ)
		{
			stats->oversize++;
		}
		else
		{
			int			i;

			for (i = 0; i < stats->nranges; i++)
			{
				if (stats->ranges[i].from <= compressed_size
					&& compressed_size <= stats->ranges[i].to)
				{
					if (O_PAGE_IS(p, LEAF))
						stats->ranges[i].leaf_count++;
					else
						stats->ranges[i].node_count++;
					break;
				}
			}
		}
	}
	PG_CATCH();
	{
		stats->errors++;
	}
	PG_END_TRY();

	context->index--;
}

void
check_btree_compression(BTreeDescr *desc, BTreeCompressStats *stats, OCompress lvl)
{
	OBTreeFindPageContext context;
	bool		recovery = is_recovery_in_progress();

	o_tables_rel_lock_extended(&desc->oids, AccessShareLock, recovery);
	o_btree_load_shmem(desc);
	init_page_find_context(&context, desc, COMMITSEQNO_INPROGRESS, BTREE_PAGE_FIND_MODIFY);

	btree_check_compression_recursive(desc, stats, lvl, &context, desc->rootInfo.rootPageBlkno);

	o_tables_rel_unlock_extended(&desc->oids, AccessShareLock, recovery);
}
