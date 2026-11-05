/*-------------------------------------------------------------------------
 *
 * check.c
 *		Routines for checking OrioleDB B-tree.
 *
 * Copyright (c) 2021-2026, Oriole DB Inc.
 * Copyright (c) 2025-2026, Supabase Inc.
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
#include "catalog/sys_trees.h"
#include "checkpoint/checkpoint.h"
#include "recovery/recovery.h"
#include "tableam/descr.h"
#include "utils/compress.h"
#include "utils/memdebug.h"
#include "utils/page_pool.h"
#include "utils/seq_buf.h"
#include "utils/ucm.h"

#include "pgstat.h"
#include "access/transam.h"
#include "common/pg_prng.h"
#include "executor/tuptable.h"
#include "lib/bloomfilter.h"
#include "storage/latch.h"
#include "tuple/slot.h"
#include "utils/guc.h"

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
	bool		emit_transient_notices;
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
									   ExtentsArray *free_extents,
									   bool compressed, bool should_exists);
static void get_free_extents_from_seqbuf_pending(SeqBufDescPrivate *seqBuf,
												 SeqBufTag *expected_tag,
												 ExtentsArray *free_extents,
												 bool compressed);
static void decode_free_extent_buf(const char *buf, Size len,
								   ExtentsArray *free_extents, bool compressed);
static bool is_sorted_by_off(ExtentsArray *array);
static bool is_sorted_by_len_off(ExtentsArray *array);

typedef void (*BTreeLeafItemCallback) (BTreeDescr *desc, Page page,
									   BTreePageItemLocator *loc, void *arg);

typedef struct BTreeStructureCheck
{
	const char *name;
	BTreeDescr *descr;
	bool		parentcheck;
	bool		rootdescend;
	BTreeCheckReport report;
	/* Path from the root, for load_page(); every check needs it.  */
	OBTreeFindPageContext context;

	/*
	 * For the searches of rootdescend.  A find context holds two page images,
	 * so it is only allocated when rootdescend is requested.
	 */
	OBTreeFindPageContext *rootDescendFindContext;
	/* Optional callback invoked for each item on leaf pages.  */
	BTreeLeafItemCallback leaf_item_cb;
	void	   *leaf_item_arg;
	/* Statistics.  */
	int			npages;
} BTreeStructureCheck;

/*
 * What the parent of a page expects from it.
 */
typedef struct BTreePageBounds
{
	int			depth;
	int			level;
	bool		leftmost;
	OFixedKey	lokey;
	OFixedKey	hikey;
} BTreePageBounds;

typedef enum CheckSubtreeResult
{
	/* Child and its subtree are checked.  */
	CheckSubtreeChecked,
	/* Child disappered (evicted or merged), next_key should be moved.  */
	CheckSubtreeDisappeared,
	/* Corruption found.  */
	CheckSubtreeCorrupted
} CheckSubtreeResult;

static CheckSubtreeResult check_subtree(BTreeStructureCheck *state,
										OInMemoryBlkno blkno, Page page,
										uint64 change_count,
										BTreePageBounds *bounds,
										OFixedKey *caller_next_key);

bool
check_btree(BTreeDescr *desc, bool force_file_check, bool wait_for_checkpoint)
{
	bool		is_sys_tree = IS_SYS_TREE_OIDS(desc->oids);
	BTreeMetaPage *metaPageBlkno = BTREE_GET_META(desc);
	BTreeCheckStatus status;
	ExtentsArray free_extents;
	uint64		data_file_len = pg_atomic_read_u64(&metaPageBlkno->datafileLength[0]);	/* Fix for S3 mode */
	bool		is_compressed = OCompressIsValid(desc->compress);
	uint32		checkpoint_number = 0;
	bool		copy_blkno;

	memset(&status, 0, sizeof(BTreeCheckStatus));
	memset(&free_extents, 0, sizeof(ExtentsArray));

	/* get busy file extents */
	status.desc = desc;
	status.hasError = false;

	/*
	 * Legacy debug entry points (wait_for_checkpoint == false) keep the
	 * BROKEN_SPLIT NOTICE; the user-facing verify_orioledb path stays silent
	 * on that transient.
	 */
	status.emit_transient_notices = !wait_for_checkpoint;
	init_page_find_context(&status.context, desc, COMMITSEQNO_INPROGRESS, BTREE_PAGE_FIND_MODIFY);

	/*
	 * get_checkpoint_number() returns false while the checkpointer holds the
	 * per-tree state.  With wait_for_checkpoint, retry until it moves past;
	 * otherwise the caller is a debug entry point and gets the legacy NOTICE.
	 */
	while (!get_checkpoint_number(desc, desc->rootInfo.rootPageBlkno,
								  &checkpoint_number, &copy_blkno))
	{
		if (!wait_for_checkpoint)
		{
			elog(NOTICE, "Tree is under checkpoint now");
			return false;
		}

		/*
		 * Holding the per-tree lock here would block the checkpointer we are
		 * waiting on, so drop it across the latch wait and reacquire after.
		 * Sys trees serialize against the checkpointer via oSysTreesLock.
		 */
		if (is_sys_tree)
			LWLockRelease(&checkpoint_state->oSysTreesLock);
		else
			o_tables_rel_unlock_extended(&desc->oids, AccessExclusiveLock, true);
		(void) WaitLatch(MyLatch,
						 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						 10L, WAIT_EVENT_PG_SLEEP);
		ResetLatch(MyLatch);
		CHECK_FOR_INTERRUPTS();
		if (is_sys_tree)
			LWLockAcquire(&checkpoint_state->oSysTreesLock, LW_EXCLUSIVE);
		else
			o_tables_rel_lock_extended(&desc->oids, AccessExclusiveLock, true);
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
		elog(NOTICE, "Not used file blocks from " UINT64_FORMAT " to " UINT64_FORMAT,
			 status.busy.blocksCount + free_extents.blocksCount,
			 data_file_len);
		status.hasError = true;
	}
	else if (data_file_len < status.busy.blocksCount + free_extents.blocksCount)
	{
		elog(NOTICE, "Excess file blocks from " UINT64_FORMAT " to " UINT64_FORMAT,
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
		bool		found;

		memset(&map_extents, 0, sizeof(ExtentsArray));
		memset(&tmp_extents, 0, sizeof(ExtentsArray));

		tag.key.oids = desc->oids;
		tag.key.oids.spcoid = desc->oids.spcoid;
		tag.num = o_get_latest_chkp_num(desc->oids.datoid,
										desc->oids.relnode,
										desc->oids.spcoid,
										checkpoint_number - 1,
										&found);
		tag.type = 'm';


		if (seq_buf_file_exist(&tag))
		{
			get_free_extents_from_file(&tag, sizeof(CheckpointFileHeader),
									   &map_extents, is_compressed, false);
		}
		else if (found)
		{
			elog(NOTICE, "%s does not exist", get_seq_buf_filename(&tag));
			status.hasError = true;
		}

		tag.type = 't';
		if (!is_compressed && seq_buf_file_exist(&tag))
		{
			get_free_extents_from_file(&tag, 0, &tmp_extents, false, false);
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

	chkp_tag.key.oids = desc->oids;
	chkp_tag.key.oids.spcoid = desc->oids.spcoid;

	if (force_file_check)
	{
		bool		found;
		uint32		num;
		uint32		map_num;

		/*
		 * Reads free blocks from map file.
		 */
		chkp_tag.type = 'm';
		chkp_tag.num = o_get_latest_chkp_num(desc->oids.datoid,
											 desc->oids.relnode,
											 desc->oids.spcoid,
											 chkp_num,
											 &found);

		get_free_extents_from_file(&chkp_tag, sizeof(CheckpointFileHeader),
								   free_extents, is_compressed, found);

		/*
		 * The map file only knows what was free when it was written.  Every
		 * extent freed since -- by the bgwriter rewriting a page, by a merge,
		 * by a copy-blkno rewrite -- is in a .tmp file or still buffered in
		 * the matching desc->tmpBuf[], and without them each of those shows
		 * up as an "Extent ... is neither free nor busy" against a tree that
		 * is perfectly sound.  The branch below already reads both for the
		 * non-forced path; do the same here.
		 */
		map_num = chkp_tag.num;
		for (num = map_num; num <= chkp_num; num++)
		{
			SeqBufTag	tmp_tag = chkp_tag;

			tmp_tag.num = num + 1;
			tmp_tag.type = 't';
			get_free_extents_from_file(&tmp_tag, 0, free_extents,
									   is_compressed, false);
			get_free_extents_from_seqbuf_pending(&desc->tmpBuf[(num + 1) % 2],
												 &tmp_tag, free_extents,
												 is_compressed);
		}
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

		get_free_extents_from_file(&chkp_tag, freebuf_offset, free_extents, false, false);

		/*
		 * Read every .tmp file whose data could still feed the free-extent
		 * stream: anything written for the previous checkpoint (already
		 * flushed) up to chkp_num + 1 (the "next" checkpoint accepting fresh
		 * frees from in-flight merges and from copy-blkno page rewrites).
		 * Mirrors the compressed branch below, which already extends the
		 * range to chkp_num + 1.  Stopping at chkp_num leaves out frees that
		 * are sitting in chkp.{chkp_num+1}.tmp and produces a phantom "Extent
		 * ... is neither free nor busy" report.
		 *
		 * For each .tmp file we also drain the matching desc->tmpBuf[] in
		 * shared memory.  free_extent_for_checkpoint() buffers small writes
		 * in an 8 KB seq_buf page; until the chunk fills or the checkpoint
		 * finalises, the bytes are not visible via the on-disk file read.
		 * Appending them here closes that read-side race.
		 */
		for (num = chkp_tag.num; num <= chkp_num; num++)
		{
			chkp_tag.num = num + 1;
			chkp_tag.type = 't';
			get_free_extents_from_file(&chkp_tag, 0, free_extents, false, false);
			get_free_extents_from_seqbuf_pending(&desc->tmpBuf[(num + 1) % 2],
												 &chkp_tag, free_extents,
												 false);
		}
	}
	else
	{
		/*
		 * Reads free blocks as normal process for compressed index.
		 * foreach_free_extent reads from in-memory free extent trees, which
		 * contain extents from consumed .tmp files.  We also need to read any
		 * unconsumed .tmp files.
		 */
		BTreeMetaPage *metaPage = BTREE_GET_META(desc);
		uint32		num;
		uint32		consumed_num = metaPage->freeBuf.tag.num;

		foreach_free_extent(desc, foreach_extent_append, (void *) free_extents);
		for (num = consumed_num + 1; num <= chkp_num + 1; num++)
		{
			chkp_tag.num = num;
			chkp_tag.type = 't';
			get_free_extents_from_file(&chkp_tag, 0, free_extents, true, false);
			/* Mirror the uncompressed-path append (see comment above). */
			get_free_extents_from_seqbuf_pending(&desc->tmpBuf[num % 2],
												 &chkp_tag, free_extents,
												 true);
		}
	}
}

/*
 * Decode a buffer of contiguous extent records into the free_extents array.
 * `len` is the number of bytes in `buf` to decode and must be a multiple of
 * the on-disk record size for this index.
 */
static void
decode_free_extent_buf(const char *buf, Size len, ExtentsArray *free_extents,
					   bool compressed)
{
	FileExtent	extent;
	uint32		off;
	Size		i = 0;

	while (i < len)
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
}

/*
 * Appends file extents from file to the free extents array.
 */
static void
get_free_extents_from_file(SeqBufTag *tag, off_t offset,
						   ExtentsArray *free_extents, bool compressed,
						   bool should_exists)
{
	char		buf[ORIOLEDB_BLCKSZ],
			   *filename;
	File		file;
	off_t		bytes_read;

	filename = get_seq_buf_filename(tag);
	file = PathNameOpenFile(filename, O_RDONLY | PG_BINARY);
	if (file == -1)
	{
		if (should_exists)
			ereport(NOTICE, (errcode_for_file_access(),
							 errmsg("could not open map file %s: %m", filename)));
		pfree(filename);
		return;
	}

	do
	{
		bytes_read = OFileRead(file, buf, ORIOLEDB_BLCKSZ, offset,
							   WAIT_EVENT_DATA_FILE_READ);
		offset += bytes_read;
		decode_free_extent_buf(buf, bytes_read, free_extents, compressed);
	} while (bytes_read == ORIOLEDB_BLCKSZ);

	FileClose(file);
	pfree(filename);
}

/*
 * Appends in-memory free-extent records that have been written to the given
 * write-mode seq_buf but are not yet flushed to its on-disk file.  Pair with
 * get_free_extents_from_file() to cover both halves of the stream when the
 * caller wants a snapshot that includes mid-checkpoint writes.
 */
static void
get_free_extents_from_seqbuf_pending(SeqBufDescPrivate *seqBuf,
									 SeqBufTag *expected_tag,
									 ExtentsArray *free_extents,
									 bool compressed)
{
	char	   *buf;
	Size		len;

	if (!SEQ_BUF_SHARED_EXIST(seqBuf->shared))
		return;
	if (!SeqBufTagEqual(&seqBuf->shared->tag, expected_tag))
		return;

	buf = palloc(seq_buf_max_pending_data_size());
	len = seq_buf_snapshot_pending_data(seqBuf, buf);
	if (len > 0)
		decode_free_extent_buf(buf, len, free_extents, compressed);
	pfree(buf);
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
				elog(NOTICE, "Extent %lu %u is neither free nor busy",
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
				elog(NOTICE, "Extent %lu %u is neither free nor busy",
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
	bool		sorted = true;

	if (array->size > 1)
	{
		for (i = 1; i < array->size && sorted; i++)
		{
			if (array->extents[i - 1].len != array->extents[i].len)
				sorted = array->extents[i - 1].len > array->extents[i].len;
			else
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
	BTreePageHeader *header = (BTreePageHeader *) p;
	OrioleDBPageDesc *page_desc = O_GET_IN_MEMORY_PAGEDESC(blkno);
	OBTreeFindPageContext *context = &status->context;
	FileExtent	extent;
	uint64		rightLink;

	Assert(OInMemoryBlknoIsValid(blkno));

	lock_page(blkno);
	if (OInMemoryBlknoIsValid(parentPagenum))
		unlock_page(parentPagenum);

	context->index++;
	context->items[context->index].blkno = blkno;
	context->items[context->index].pageChangeCount = O_PAGE_GET_CHANGE_COUNT(p);

	rightLink = header->rightLink;
	if (RightLinkIsValid(rightLink))
	{
		Page		rightP = O_GET_IN_MEMORY_PAGE(RIGHTLINK_GET_BLKNO(rightLink));

		if (O_PAGE_IS(rightP, BROKEN_SPLIT) && status->emit_transient_notices)
		{
			elog(NOTICE, "BTree has a broken split.");
			status->hasError = true;
		}
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
	size_t		compressed_size;

	ppool_ucm_inc_usage(desc->ppool, blkno);

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
	VALGRIND_MAKE_MEM_DEFINED(buf, ORIOLEDB_BLCKSZ);

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

static uint64
get_page_state_change_count(OInMemoryBlkno blkno)
{
	Page		p = O_GET_IN_MEMORY_PAGE(blkno);

	return pg_atomic_read_u64(&O_PAGE_HEADER(p)->state) &
		PAGE_STATE_CHANGE_COUNT_MASK;
}

static uint64
read_locked_page(Page dest, OInMemoryBlkno blkno)
{
	memcpy(dest, O_GET_IN_MEMORY_PAGE(blkno), ORIOLEDB_BLCKSZ);
	return get_page_state_change_count(blkno);
}

static void
copy_page_hikey(BTreeDescr *desc, OFixedKey *dest, const Page p)
{
	if (O_PAGE_IS(p, RIGHTMOST))
		clear_fixed_key(dest);
	else
		copy_fixed_hikey(desc, dest, p);
}

static void
structure_corrupted(const BTreeStructureCheck *state, OInMemoryBlkno blkno,
					Page p, const char *msg)
{
	StringInfoData buf;

	initStringInfo(&buf);
	appendStringInfo(&buf, "%s, page %u (level %d): %s",
					 state->name, blkno, (int) PAGE_GET_LEVEL(p), msg);
	state->report(buf.data);

	pfree(buf.data);
}

static void
advance_keys(BTreeStructureCheck *state, OFixedKey *key, OTuple to)
{
	BTreeDescr *desc = state->descr;

	if (O_TUPLE_IS_NULL(to))
		return;
	if (!O_TUPLE_IS_NULL(key->tuple))
	{
		if (o_btree_cmp(desc, &to, BTreeKeyNonLeafKey,
						&key->tuple, BTreeKeyNonLeafKey) <= 0)
			return;
	}
	copy_fixed_key(desc, key, to);
}

/* Returns whether our copy of the page at 'blkno' is still current.  */
static bool
page_copy_is_current(OInMemoryBlkno blkno, uint32 page_change_count,
					 uint64 change_count)
{
	bool		current;

	lock_page(blkno);
	current = O_GET_IN_MEMORY_PAGE_CHANGE_COUNT(blkno) == page_change_count &&
		get_page_state_change_count(blkno) == change_count;
	unlock_page(blkno);

	return current;
}

static void
get_chunk_bounds(const Page page, int chunk, OffsetNumber *items_count,
				 LocationIndex *end)
{
	BTreePageHeader *header = (BTreePageHeader *) page;

	if (chunk == header->chunksCount - 1)
	{
		*items_count = header->itemsCount - header->chunkDesc[chunk].offset;
		*end = header->dataSize;
	}
	else
	{
		*items_count = header->chunkDesc[chunk + 1].offset -
			header->chunkDesc[chunk].offset;
		*end = SHORT_GET_LOCATION(header->chunkDesc[chunk + 1].shortLocation);
	}
}

static bool
check_page_layout(const BTreeStructureCheck *state, OInMemoryBlkno blkno,
				  const Page page)
{
	BTreePageHeader *header = (BTreePageHeader *) page;
	int			i;

	if (header->dataSize > ORIOLEDB_BLCKSZ)
	{
		structure_corrupted(state, blkno, page,
							psprintf("data size %u is out of the range",
									 header->dataSize));
		return false;
	}
	if (header->hikeysEnd > header->dataSize)
	{
		structure_corrupted(state, blkno, page,
							psprintf("hikeys section end %u is out of range %u",
									 header->hikeysEnd, header->dataSize));
		return false;
	}
	if (header->chunksCount == 0 ||
		header->chunksCount > BTREE_PAGE_MAX_CHUNK_ITEMS)
	{
		structure_corrupted(state, blkno, page,
							psprintf("chunk count %u is invalid",
									 header->chunksCount));
		return false;
	}
	if (header->itemsCount > BTREE_PAGE_MAX_ITEMS)
	{
		structure_corrupted(state, blkno, page,
							psprintf("items count %u is out of range %lu",
									 header->itemsCount, BTREE_PAGE_MAX_ITEMS));
		return false;
	}
	if (!O_PAGE_IS(page, LEAF) && header->itemsCount == 0)
	{
		structure_corrupted(state, blkno, page, "non-leaf page has no downlink");
		return false;
	}
	if (header->chunkDesc[0].offset != 0)
	{
		structure_corrupted(state, blkno, page,
							psprintf("first chunk starts at item %u",
									 header->chunkDesc[0].offset));
		return false;
	}
	for (i = 0; i < header->chunksCount; ++i)
	{
		BTreePageChunkDesc *chunk = &header->chunkDesc[i];
		BTreePageChunk *chunk_data;
		OffsetNumber chunk_items_count;
		LocationIndex end;
		LocationIndex start = SHORT_GET_LOCATION(chunk->shortLocation);
		int			chunk_it;

		if (chunk->offset > header->itemsCount)
		{
			structure_corrupted(state, blkno, page,
								psprintf("chunk %d starts at itum %u, beyond"
										 "the %u items of the page",
										 i, chunk->offset, header->itemsCount));
			return false;
		}

		/* Check that chunks are layed in right order.  */
		if (i < header->chunksCount - 1)
		{
			BTreePageChunkDesc *next = &header->chunkDesc[i + 1];

			if (next->offset < chunk->offset ||
				next->shortLocation < chunk->shortLocation ||
				next->hikeyShortLocation < chunk->hikeyShortLocation)
			{
				structure_corrupted(state, blkno, page,
									psprintf("chunks %d and %d are out of order",
											 i, i + 1));
				return false;
			}
		}

		/* The only last chunk of RIGHTMOST page doesn't have hikey.  */
		if ((!O_PAGE_IS(page, RIGHTMOST) || i != (header->chunksCount - 1)) &&
			SHORT_GET_LOCATION(chunk->hikeyShortLocation) >= header->hikeysEnd)
		{
			structure_corrupted(state, blkno, page,
								psprintf("hikey of chunk %d is out of the "
										 "hikeys segment", i));
			return false;
		}

		get_chunk_bounds(page, i, &chunk_items_count, &end);
		if (start < header->hikeysEnd)
		{
			structure_corrupted(state, blkno, page,
								psprintf("start %u of the chunk %u is below "
										 "the end %u of hikey segment",
										 start, i, header->hikeysEnd));
			return false;
		}
		if (end > header->dataSize)
		{
			structure_corrupted(state, blkno, page,
								psprintf("end %u of the chunk %u is  "
										 "out of range %u of the page",
										 end, i, header->dataSize));
			return false;
		}
		if (start > end)
		{
			structure_corrupted(state, blkno, page,
								psprintf("start %u of the chunk %u is above "
										 "its end %u", start, i, end));
			return false;
		}

		if (MAXALIGN(sizeof(LocationIndex) * chunk_items_count) > end - start)
		{
			structure_corrupted(state, blkno, page,
								psprintf("%u items can't fit into chunk %i "
										 "which size is %u",
										 chunk_items_count, i, end - start));
			return false;
		}

		chunk_data = (BTreePageChunk *) ((Pointer) page + start);
		for (chunk_it = 0; chunk_it < chunk_items_count; ++chunk_it)
		{
			LocationIndex offset = ITEM_GET_OFFSET(chunk_data->items[chunk_it]);

			if (offset < MAXALIGN(sizeof(LocationIndex) * chunk_items_count) ||
				offset > end - start)
			{
				structure_corrupted(state, blkno, page,
									psprintf("item %d is out of its chunk %d",
											 chunk_it, i));
				return false;
			}

			if (chunk_it > 0 &&
				offset < ITEM_GET_OFFSET(chunk_data->items[chunk_it - 1]))
			{
				structure_corrupted(state, blkno, page,
									psprintf("items %d and %d of chunk %d are "
											 "out of order",
											 chunk->offset + chunk_it - 1,
											 chunk->offset + chunk_it,
											 i));
				return false;
			}
		}
	}

	return true;
}

static bool
check_page_keys(const BTreeStructureCheck *state, OInMemoryBlkno blkno,
				const Page page)
{
	BTreePageHeader *header = (BTreePageHeader *) page;
	BTreeDescr *desc = state->descr;
	OTuple		prev_tuple;
	int			i;
	bool		is_leaf = O_PAGE_IS(page, LEAF);
	BTreeKeyType key_type = is_leaf ? BTreeKeyLeafTuple : BTreeKeyNonLeafKey;

	O_TUPLE_SET_NULL(prev_tuple);

	for (i = 0; i < header->chunksCount; ++i)
	{
		BTreePageChunkDesc *chunk = &header->chunkDesc[i];
		LocationIndex start = SHORT_GET_LOCATION(chunk->shortLocation);
		LocationIndex end;
		BTreePageChunk *chunk_data;
		OffsetNumber chunk_items_count;
		OTuple		chunk_hikey;
		int			chunk_it;

		if (O_PAGE_IS(page, RIGHTMOST) && i == (header->chunksCount - 1))
			O_TUPLE_SET_NULL(chunk_hikey);
		else
		{
			chunk_hikey.formatFlags = chunk->hikeyFlags;
			chunk_hikey.data = (Pointer) page +
				SHORT_GET_LOCATION(chunk->hikeyShortLocation);
		}

		get_chunk_bounds(page, i, &chunk_items_count, &end);
		chunk_data = (BTreePageChunk *) ((Pointer) page + start);
		for (chunk_it = 0; chunk_it < chunk_items_count; ++chunk_it)
		{
			LocationIndex offset = ITEM_GET_OFFSET(chunk_data->items[chunk_it]);
			LocationIndex item_end;
			OTuple		tuple;
			int			len;
			int			itemno = chunk->offset + chunk_it;

			item_end = (chunk_it < chunk_items_count - 1) ?
				ITEM_GET_OFFSET(chunk_data->items[chunk_it + 1]) : end - start;
			tuple.formatFlags = ITEM_GET_FLAGS(chunk_data->items[chunk_it]);
			if (is_leaf)
			{
				tuple.data = (Pointer) chunk_data + offset + BTreeLeafTuphdrSize;
				len = BTreeLeafTuphdrSize + o_btree_len(desc, tuple, OTupleLength);
			}
#ifdef ORIOLEDB_CUT_FIRST_KEY
			else if (itemno == 0)
			{
				/* The first key of a non-leaf page is cut.  */
				O_TUPLE_SET_NULL(tuple);
				len = BTreeNonLeafTuphdrSize;
			}
#endif
			else
			{
				tuple.data = (Pointer) chunk_data + offset + BTreeNonLeafTuphdrSize;
				len = BTreeNonLeafTuphdrSize + o_btree_len(desc, tuple, OKeyLength);
			}

			if (offset + len > item_end)
			{
				structure_corrupted(state, blkno, page,
									psprintf("item %d of length %d doesn't fit"
											 " its space", chunk_it, len));
				return false;
			}

			/* First key of non-leaf page.  */
			if (O_TUPLE_IS_NULL(tuple))
				continue;

			if (!O_TUPLE_IS_NULL(chunk_hikey) &&
				o_btree_cmp(desc, &tuple, key_type,
							&chunk_hikey, BTreeKeyNonLeafKey) >= 0)
			{
				structure_corrupted(state, blkno, page,
									psprintf("item %d is not below the hikey "
											 "of its chunk", itemno));
				return false;
			}

			if (!O_TUPLE_IS_NULL(prev_tuple) &&
				o_btree_cmp(desc, &prev_tuple, key_type, &tuple, key_type) >= 0)
			{
				structure_corrupted(state, blkno, page,
									psprintf("item %d is not above the previous item",
											 itemno));
				return false;
			}

			prev_tuple = tuple;
		}
	}

	return true;
}

static bool
check_page_bounds(const BTreeStructureCheck *state, OInMemoryBlkno blkno,
				  const Page page, BTreePageBounds *bounds)
{
	BTreeDescr *desc = state->descr;
	bool		splitting = RightLinkIsValid(BTREE_PAGE_GET_RIGHTLINK(page));

	if (PAGE_GET_LEVEL(page) != bounds->level)
	{
		structure_corrupted(state, blkno, page,
							psprintf("page is expected on level %d",
									 bounds->level));
		return false;
	}

	if (O_PAGE_IS(page, LEFTMOST) != bounds->leftmost)
	{
		structure_corrupted(state, blkno, page,
							bounds->leftmost ?
							"leftmost page is not marked leftmost" :
							"page is marked leftmost, but it it isn't");
		return false;
	}

	if (O_TUPLE_IS_NULL(bounds->hikey.tuple) && !splitting &&
		!O_PAGE_IS(page, RIGHTMOST))
	{
		structure_corrupted(state, blkno, page,
							"rightmost page is not marked rightmost");
		return false;
	}

	if (!O_TUPLE_IS_NULL(bounds->hikey.tuple) && O_PAGE_IS(page, RIGHTMOST))
	{
		structure_corrupted(state, blkno, page,
							"page is marked rightmost, but it isn't rightmost");
		return false;
	}

	if (!O_TUPLE_IS_NULL(bounds->hikey.tuple))
	{
		OTuple		hikey;
		int			cmp;

		BTREE_PAGE_GET_HIKEY(hikey, page);
		cmp = o_btree_cmp(desc, &hikey, BTreeKeyNonLeafKey,
						  &bounds->hikey.tuple, BTreeKeyNonLeafKey);
		if (splitting ? cmp >= 0 : cmp != 0)
		{
			structure_corrupted(state, blkno, page,
								splitting ?
								"hikey is not below the next key of the parent" :
								"hikey doesn't match the next key of the parent");
			return false;
		}
	}

	if (!O_TUPLE_IS_NULL(bounds->lokey.tuple))
	{
		BTreePageItemLocator loc;
		OTuple		first;

		BTREE_PAGE_LOCATOR_FIRST(page, &loc);
		O_TUPLE_SET_NULL(first);
		if (O_PAGE_IS(page, LEAF))
		{
			if (BTREE_PAGE_LOCATOR_IS_VALID(page, &loc))
				BTREE_PAGE_READ_LEAF_TUPLE(first, page, &loc);
		}
		else
		{
			/*
			 * Skip the first item: its key stands for minus infinity (or cut)
			 * by ORIOLEDB_CUT_FIRST_KEY.
			 */
			BTREE_PAGE_LOCATOR_NEXT(page, &loc);
			if (BTREE_PAGE_LOCATOR_IS_VALID(page, &loc))
				BTREE_PAGE_READ_INTERNAL_TUPLE(first, page, &loc);
		}

		if (!O_TUPLE_IS_NULL(first) &&
			o_btree_cmp(desc, &first,
						O_PAGE_IS(page, LEAF) ? BTreeKeyLeafTuple : BTreeKeyNonLeafKey,
						&bounds->lokey.tuple, BTreeKeyNonLeafKey) < 0)
		{
			structure_corrupted(state, blkno, page,
								"first item is below the key of the parent");
			return false;
		}
	}

	return true;
}

static bool
check_page(BTreeStructureCheck *state, OInMemoryBlkno blkno, const Page page,
		   BTreePageBounds *bounds)
{
	state->npages++;

	if (!check_page_layout(state, blkno, page) ||
		!check_page_keys(state, blkno, page))
		return false;

	if (bounds != NULL && state->parentcheck &&
		!check_page_bounds(state, blkno, page, bounds))
		return false;

	return true;
}

/*
 * The right sibling of a page being split has downlink in the parent yet:
 * visit it, and its own right siblings.
 * Split doesn't wait for an earlier unfinished split of the same page, so
 * chains of right links can form.
 */
static CheckSubtreeResult
check_right_siblings(BTreeStructureCheck *state, OInMemoryBlkno blkno,
					 Page page, uint64 change_count, BTreePageBounds *bounds,
					 OFixedKey *next_key)
{
	BTreeDescr *desc = state->descr;

	while (true)
	{
		uint64		rightlink,
					right_changecount;
		OInMemoryBlkno right_blkno;
		CheckSubtreeResult res;

		lock_page(blkno);

		/*
		 * If the page is gone, we can't follow its rightlink anymore.  We
		 * visited it up to the hikey of out last copy, which we keep. This
		 * situation is ok, we never merge/evict any pages with an unfinished
		 * split.
		 */
		if (O_GET_IN_MEMORY_PAGE_CHANGE_COUNT(blkno) !=
			O_PAGE_GET_CHANGE_COUNT(page))
		{
			unlock_page(blkno);
			return CheckSubtreeChecked;
		}

		if (get_page_state_change_count(blkno) != change_count)
			change_count = read_locked_page(page, blkno);

		rightlink = BTREE_PAGE_GET_RIGHTLINK(page);
		if (!RightLinkIsValid(rightlink))
		{
			/* All splits are finished.  */
			unlock_page(blkno);
			return CheckSubtreeChecked;
		}

		right_blkno = RIGHTLINK_GET_BLKNO(rightlink);
		lock_page(right_blkno);

		if (O_GET_IN_MEMORY_PAGE_CHANGE_COUNT(right_blkno) !=
			RIGHTLINK_GET_CHANGECOUNT(rightlink))
		{
			unlock_page(right_blkno);
			unlock_page(blkno);

			structure_corrupted(state, blkno, page,
								"rightlink points to a page that is not the "
								"part of the tree");
			return CheckSubtreeCorrupted;
		}

		copy_fixed_hikey(desc, &bounds->lokey, page);
		bounds->leftmost = false;
		advance_keys(state, next_key, bounds->lokey.tuple);

		right_changecount = read_locked_page(page, right_blkno);
		unlock_page(right_blkno);
		unlock_page(blkno);

		res = check_subtree(state, right_blkno, page, right_changecount,
							bounds, next_key);

		if (res != CheckSubtreeChecked)
			return res;

		/*---
		 * Move to right block and repeat operation with next rightlink, e.g.
		 * 		C --- rightlink ---> C' --- rightlink ---> C''
		 */
		blkno = right_blkno;
		change_count = right_changecount;
	}
}

/*
 * Searches of each tuple of a leaf page from the root, and checks that the
 * search finds it.
 */
static bool
check_leaf_rootdescend(BTreeStructureCheck *state, OInMemoryBlkno blkno,
					   Page page)
{
	BTreeDescr *desc = state->descr;
	OBTreeFindPageContext *context = state->rootDescendFindContext;
	BTreePageItemLocator loc;

	BTREE_PAGE_FOREACH_ITEMS(page, &loc)
	{
		BTreePageItemLocator *found_loc;
		OTuple		tuple,
					found;

		BTREE_PAGE_READ_LEAF_TUPLE(tuple, page, &loc);

		init_page_find_context(context, desc, COMMITSEQNO_INPROGRESS,
							   BTREE_PAGE_FIND_IMAGE);
		if (find_page(context, &tuple, BTreeKeyLeafTuple, 0) !=
			OFindPageResultSuccess)
		{
			/* The search could not complete: that's not a corruption.  */
			break;
		}

		found_loc = &context->items[context->index].locator;
		O_TUPLE_SET_NULL(found);
		if (BTREE_PAGE_LOCATOR_IS_VALID(context->img, found_loc))
			BTREE_PAGE_READ_LEAF_TUPLE(found, context->img, found_loc);
		if (O_TUPLE_IS_NULL(found) ||
			o_btree_cmp(desc, &found, BTreeKeyLeafTuple,
						&tuple, BTreeKeyLeafTuple) != 0)
		{
			structure_corrupted(state, blkno, page,
								psprintf("item %d can't be found by a search "
										 "from the root",
										 BTREE_PAGE_LOCATOR_GET_OFFSET(page, &loc)));
			return false;
		}
	}

	return true;
}

static bool
page_belongs_to_tree(const BTreeStructureCheck *state, OInMemoryBlkno blkno)
{
	OrioleDBPageDesc *page_descr = O_GET_IN_MEMORY_PAGEDESC(blkno);

	return ORelOidsIsEqual(page_descr->oids, state->descr->oids);
}

static CheckSubtreeResult
check_subtree(BTreeStructureCheck *state, OInMemoryBlkno blkno, Page page,
			  uint64 change_count, BTreePageBounds *bounds,
			  OFixedKey *caller_next_key)
{
	Page		child;
	BTreeDescr *desc = state->descr;
	OFixedKey	page_hikey;
	OFixedKey	next_key;
	CheckSubtreeResult res = CheckSubtreeChecked;
	uint64		page_change_count = O_PAGE_GET_CHANGE_COUNT(page);

	if (!check_page(state, blkno, page, bounds))
		return CheckSubtreeCorrupted;

	if (O_PAGE_IS(page, LEAF))
	{
		if (state->rootdescend && !check_leaf_rootdescend(state, blkno, page))
			return CheckSubtreeCorrupted;
		if (state->leaf_item_cb)
		{
			BTreePageItemLocator loc;

			BTREE_PAGE_FOREACH_ITEMS(page, &loc)
				state->leaf_item_cb(state->descr, page, &loc,
									state->leaf_item_arg);
		}
		return CheckSubtreeChecked;
	}

	if (bounds->depth + 1 >= ORIOLEDB_MAX_DEPTH)
	{
		structure_corrupted(state, blkno, page, "tree is too deep");
		return CheckSubtreeCorrupted;
	}

	/*
	 * Move forward to child.
	 *
	 * 'next_key' is the first key NOT VISITED YET.  We look up the child it
	 * belongs to in the latest copy of the page, so that we go on from the
	 * right place even if the page changes meanwhile.
	 */
	child = palloc(ORIOLEDB_BLCKSZ);
	clear_fixed_key(&next_key);
	copy_page_hikey(desc, &page_hikey, page);
	while (true)
	{
		BTreePageItemLocator loc;
		OffsetNumber offset;
		Page		cur;
		uint64		downlink;
		BTreeNonLeafTuphdr *tuphdr;
		uint64		child_change_count;
		OInMemoryBlkno child_blkno;
		BTreePageBounds child_bounds;
		CheckSubtreeResult child_res;

		if (O_TUPLE_IS_NULL(next_key.tuple))
			BTREE_PAGE_LOCATOR_FIRST(page, &loc);
		else if (!O_PAGE_IS(page, RIGHTMOST) &&
				 o_btree_cmp(desc, &next_key.tuple, BTreeKeyNonLeafKey,
							 &page_hikey.tuple, BTreeKeyNonLeafKey) >= 0)
		{
			/*---
			 * Concurrent split happenned; the rest of its keys moved to its
			 * right sibling, which our caller visits.  E.g.:
			 *
			 *      Grandparent [-inf; P]
			 *               |
			 *               v
			 * P: [ -inf; C1 | 10; C2 | 20; C3 | 30; C4 ] (rightmost, no hikey)
			 *
			 * Visit C1, then nextKey = C1.hikey = 10 -> visit C2.  While we're
			 * inside C2 subtree, P is not locked, and P can be splitted, e.g.
			 * at key 20:
			 *
			 *    Grandparent [ -inf; P | 20; P' ]
			 *         /                \
			 *        P -- rightlink --> P'
			 *
			 * P  [ -inf; C1 | 10; C2 ] hikey 20
			 * P' [   20; C3 | 30; C4 ] (rightmost, no hikey)
			 *
			 * Then the Grandparent's loop at check_subtree() reaches P' by
			 * P.hikey = 20 (after fresh copying the newest P), then the item
			 * before first key > 20 -> check_subtree(P')
			 */
			break;
		}
		else
		{
			/*---
			 * btree_page_search() returns the first item with val > Key.
			 * E.g. we have:
			 *
			 * item:       0        1        2        3
			 * range: [-inf; 10] [10, 20) [20, 30) [30, 40)
			 *
			 * So if we're looking for item with next_key = 10, then
			 * btree_page_search() would return the 2nd item, but we should
			 * step back here to the 1st.  The same as find_page() does.
			 */
			btree_page_search(desc, page, (Pointer) &next_key.tuple,
							  BTreeKeyNonLeafKey, NULL, &loc);
			BTREE_PAGE_LOCATOR_PREV(page, &loc);
		}

		/*
		 * The page passed check_page(), so it has downlinks, but if there's
		 * no item, the page can't be searched, what means the page is
		 * corrupted.
		 */
		if (!BTREE_PAGE_LOCATOR_IS_VALID(page, &loc))
		{
			structure_corrupted(state, blkno, page,
								"no downlink to follow for the keys not"
								" visited yet");
			res = CheckSubtreeCorrupted;
			break;
		}
		offset = BTREE_PAGE_LOCATOR_GET_OFFSET(page, &loc);

		lock_page(blkno);

		/*
		 * Check if the page is still on this slot: eviction would change page
		 * change count.
		 */
		cur = O_GET_IN_MEMORY_PAGE(blkno);
		if (O_PAGE_GET_CHANGE_COUNT(cur) != page_change_count ||
			O_PAGE_IS(cur, LEAF))
		{
			unlock_page(blkno);
			advance_keys(state, caller_next_key, next_key.tuple);
			res = CheckSubtreeDisappeared;
			break;
		}

		/*
		 * Check if the content of the page is modifed since we copied it and
		 * take a new copy.
		 */
		if (get_page_state_change_count(blkno) != change_count)
		{
			change_count = read_locked_page(page, blkno);
			unlock_page(blkno);

			if (!check_page(state, blkno, page, NULL))
			{
				res = CheckSubtreeCorrupted;
				break;
			}

			copy_page_hikey(desc, &page_hikey, page);
			continue;
		}

		tuphdr = (BTreeNonLeafTuphdr *) BTREE_PAGE_LOCATOR_GET_ITEM(page, &loc);
		downlink = tuphdr->downlink;

		if (DOWNLINK_IS_IN_IO(downlink))
		{
			unlock_page(blkno);
			wait_for_io_completion(DOWNLINK_GET_IO_LOCKNUM(downlink));
			continue;
		}

		if (DOWNLINK_IS_ON_DISK(downlink))
		{
			OBTreeFindPageContext *context = &state->context;

			/*
			 * load_page() wants the parent locked, and leaves it locked.  It
			 * may find the parent elsewhere if the parent changed.
			 */
			context->index = bounds->depth;
			context->items[context->index].blkno = blkno;
			context->items[context->index].pageChangeCount = page_change_count;
			page_item_fill_locator(cur, offset,
								   &context->items[context->index].locator);
			load_page(context);

			blkno = context->items[context->index].blkno;
			change_count = read_locked_page(page, blkno);
			page_change_count = O_PAGE_GET_CHANGE_COUNT(page);
			unlock_page(blkno);
			if (!check_page(state, blkno, page, NULL))
			{
				res = CheckSubtreeCorrupted;
				break;
			}

			copy_page_hikey(desc, &page_hikey, page);
			continue;
		}

		/*
		 * Parent is locked and unchanged, and nobody may change C without
		 * changing parent first.  So check parent's downlink is still
		 * correct.
		 */
		child_blkno = DOWNLINK_GET_IN_MEMORY_BLKNO(downlink);
		lock_page(child_blkno);
		if (O_GET_IN_MEMORY_PAGE_CHANGE_COUNT(child_blkno) !=
			DOWNLINK_GET_IN_MEMORY_CHANGECOUNT(downlink) ||
			!page_belongs_to_tree(state, child_blkno))
		{
			unlock_page(child_blkno);
			unlock_page(blkno);
			structure_corrupted(state, blkno, page,
								psprintf("downlink of item %d points to"
										 " a page that is not part of"
										 " the tree", offset));
			res = CheckSubtreeCorrupted;
			break;
		}
		child_change_count = read_locked_page(child, child_blkno);
		unlock_page(child_blkno);
		unlock_page(blkno);

		/* What the page expects of the child.  */
		child_bounds.depth = bounds->depth + 1;
		child_bounds.level = PAGE_GET_LEVEL(page) - 1;
		child_bounds.leftmost = O_PAGE_IS(page, LEFTMOST) && (offset == 0);
		if (offset == 0)
			copy_fixed_key(desc, &child_bounds.lokey, bounds->lokey.tuple);
		else
			copy_fixed_page_key(desc, &child_bounds.lokey, page, &loc);

		if (BTREE_PAGE_LOCATOR_NEXT(page, &loc))
			copy_fixed_page_key(desc, &child_bounds.hikey, page, &loc);
		else if (O_PAGE_IS(page, RIGHTMOST))
			clear_fixed_key(&child_bounds.hikey);
		else
			copy_fixed_hikey(desc, &child_bounds.hikey, page);

		child_res = check_subtree(state, child_blkno, child,
								  child_change_count, &child_bounds,
								  &next_key);

		/* Catch-up non-finished split.  */
		if (child_res == CheckSubtreeChecked)
			child_res = check_right_siblings(state, child_blkno, child,
											 child_change_count, &child_bounds,
											 &next_key);

		if (child_res == CheckSubtreeCorrupted)
		{
			res = CheckSubtreeCorrupted;
			break;
		}

		/*
		 * A page below went away meanwhile.  next_key is moved forward: look
		 * up again leads to the page where that key is now.
		 */
		else if (child_res == CheckSubtreeDisappeared)
			continue;

		/* We are obviously finished.  */
		if (O_PAGE_IS(child, RIGHTMOST))
			break;

		{
			OTuple		hikey;
			bool		progress;
			bool		done;

			BTREE_PAGE_GET_HIKEY(hikey, child);
			progress = O_TUPLE_IS_NULL(next_key.tuple) ||
				o_btree_cmp(desc, &hikey, BTreeKeyNonLeafKey,
							&next_key.tuple, BTreeKeyNonLeafKey) > 0;
			done = !O_PAGE_IS(page, RIGHTMOST) &&
				o_btree_cmp(desc, &hikey, BTreeKeyNonLeafKey,
							&page_hikey.tuple, BTreeKeyNonLeafKey) >= 0;
			if (progress)
				copy_fixed_key(desc, &next_key, hikey);
			if (!progress)
			{
				/*---
				 * When we copied P and C2:
				 * 		P [... | 20; C2 | ...]
				 * 		C2[20; 24) -- rightlink --> C2'[24, 30)
				 * Split finished since:
				 * 		P [... | 20;C2 | 24; C2' | ...]
				 * 		C2 has no right link anymore
				 *
				 */
				if (!page_copy_is_current(blkno, page_change_count, change_count))
					continue;
				structure_corrupted(state, child_blkno, child,
									"high key is not above the keys"
									" visited before");
				res = CheckSubtreeCorrupted;
				break;
			}

			if (done)
				break;
		}
	}

	pfree(child);

	return res;
}

/*
 * State for the heapallindexed check.
 */
typedef struct HeapAllIndexedState
{
	bloom_filter *filter;
	/* Secondary index info for forming tuples from primary rows.  */
	OIndexDescr *secondaryIndex;
	/* Owning table description; used to build table-ordered slots.  */
	OTableDescr *tableDescr;
	const char *secondaryName;
	const char *primaryName;
	int64		heaptuplespresent;
} HeapAllIndexedState;

/*
 * Callback: fingerprint each non-deleted item in the secondary index leaf
 * pages into the Bloom filter.  The filter represents the set of all index
 * entries; later we verify every primary row is in this set.
 */
static void
heapallindexed_fingerprint_cb(BTreeDescr *desc, Page page,
							  BTreePageItemLocator *loc, void *arg)
{
	HeapAllIndexedState *state = (HeapAllIndexedState *) arg;
	BTreeLeafTuphdr *tuphdr;
	OTuple		tuple;

	BTREE_PAGE_READ_LEAF_ITEM(tuphdr, tuple, page, loc);
	if (tuphdr->deleted)
		return;

	bloom_add_element(state->filter, (unsigned char *) tuple.data,
					  o_btree_len(desc, tuple, OTupleLength));
}

/*
 * Callback: probe each visible primary index row against the Bloom filter.
 * If a row is not found in the filter, it lacks a matching entry in the
 * secondary index.
 *
 * The primary row is materialized into a table-ordered slot and a
 * canonical secondary index tuple is formed with
 * tts_orioledb_make_secondary_tuple() -- the exact code path used when the
 * index is built.  This guarantees the probed bytes match the ones
 * fingerprinted from the secondary index.
 */
static void
heapallindexed_probe_cb(BTreeDescr *desc, Page page,
						BTreePageItemLocator *loc, void *arg)
{
	HeapAllIndexedState *state = (HeapAllIndexedState *) arg;
	BTreeLeafTuphdr *tuphdr;
	OTuple		tuple;
	OIndexDescr *sec = state->secondaryIndex;
	TupleTableSlot *slot;
	OTuple		formed;
	int			tuplelen;

	BTREE_PAGE_READ_LEAF_ITEM(tuphdr, tuple, page, loc);
	if (tuphdr->deleted)
		return;

	/*
	 * Only probe rows visible to us: committed and finished for everybody.
	 * Rows from in-progress transactions are not yet visible.
	 */
	if (!XACT_INFO_FINISHED_FOR_EVERYBODY(tuphdr->xactInfo))
		return;

	/*
	 * Build a table-ordered slot from the primary leaf tuple and form the
	 * corresponding secondary index tuple.  A fresh slot is used per row so
	 * the TTS_EMPTY assertion in tts_orioledb_store_tuple() is satisfied.
	 */
	slot = MakeSingleTupleTableSlot(state->tableDescr->tupdesc,
									&TTSOpsOrioleDB);
	tts_orioledb_store_tuple(slot, tuple, state->tableDescr,
							 COMMITSEQNO_INPROGRESS, PrimaryIndexNumber,
							 false, NULL);

	formed = tts_orioledb_make_secondary_tuple(slot, sec, true);
	tuplelen = o_btree_len(&sec->desc, formed, OTupleLength);

	ExecDropSingleTupleTableSlot(slot);

	if (bloom_lacks_element(state->filter,
							(unsigned char *) formed.data, tuplelen))
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("heap tuple lacks matching index tuple within index \"%s\"",
						state->secondaryName)));

	state->heaptuplespresent++;
	pfree(formed.data);
}

/*
 * check_btree_structure
 *
 * Verify the structural integrity of a B-tree.  When heapallindexed is
 * true, additionally verify that every visible row in the primary index
 * has a matching entry in this (secondary) index.
 *
 * The caller must have loaded desc.  If heapallindexed is true, the caller
 * must also have loaded primaryIndex->desc.
 *
 * For heapallindexed: the Bloom filter is built from the secondary index
 * tuples (desc), and each visible primary index row is probed against it.
 * A primary row missing from the filter indicates a lost index entry.
 *
 * Note: the primary index itself is also fully structure-checked during the
 * second walk, because a corrupted primary B-tree could cause us to miss
 * rows or read garbage, producing false negatives or false errors.
 */
void
check_btree_structure(BTreeDescr *desc, const char *name, bool parentcheck,
					  bool rootdescend, BTreeCheckReport report,
					  bool heapallindexed,
					  OIndexDescr *secondaryIndex, OIndexDescr *primaryIndex,
					  OTableDescr *tableDescr)
{
	BTreeStructureCheck state = {0};
	OInMemoryBlkno root_blkno;
	Page		root;
	BTreePageBounds bounds;
	uint64		change_count;
	OFixedKey	next_key;
	HeapAllIndexedState haistate;
	bool		has_hai = heapallindexed;
	int64		total_elems;
	uint64		seed;

	if (heapallindexed &&
		(primaryIndex == NULL || secondaryIndex == NULL || tableDescr == NULL))
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("heapallindexed requires secondaryIndex, primaryIndex and tableDescr")));

	if (has_hai)
	{
		total_elems = Max((int64) 1,
						  (int64) TREE_NUM_LEAF_PAGES(desc) * 100);
		seed = pg_prng_uint64(&pg_global_prng_state);

		haistate.filter = bloom_create(total_elems, maintenance_work_mem, seed);
		haistate.secondaryIndex = secondaryIndex;
		haistate.tableDescr = tableDescr;
		haistate.secondaryName = name;
		haistate.primaryName = "primary index";
		haistate.heaptuplespresent = 0;
	}

	/*
	 * Walk 1: structure check on the target tree (secondary index). If
	 * heapallindexed, also fingerprint each leaf item into the filter.
	 */
	root_blkno = desc->rootInfo.rootPageBlkno;
	state.name = name;
	state.descr = desc;
	state.parentcheck = parentcheck;
	state.rootdescend = rootdescend;
	state.report = report;
	state.leaf_item_cb = has_hai ? heapallindexed_fingerprint_cb : NULL;
	state.leaf_item_arg = has_hai ? &haistate : NULL;
	init_page_find_context(&state.context, desc, COMMITSEQNO_INPROGRESS, BTREE_PAGE_FIND_MODIFY);

	if (rootdescend)
		state.rootDescendFindContext = palloc(sizeof(OBTreeFindPageContext));

	bounds.depth = 0;
	bounds.leftmost = true;
	clear_fixed_key(&bounds.lokey);
	clear_fixed_key(&bounds.hikey);

	root = palloc(ORIOLEDB_BLCKSZ);
	lock_page(root_blkno);
	change_count = read_locked_page(root, root_blkno);
	unlock_page(root_blkno);

	bounds.level = PAGE_GET_LEVEL(root);
	(void) check_subtree(&state, root_blkno, root, change_count, &bounds, &next_key);
	pfree(root);

	elog(DEBUG1, "%s: checked %d pages", name, state.npages);

	/*
	 * Walk 2 (heapallindexed only): structure check on the primary index,
	 * probing each visible row against the Bloom filter.  The primary index
	 * is also structure-checked here because a corrupted primary tree could
	 * hide missing secondary entries.
	 */
	if (has_hai)
	{
		BTreeDescr *primaryDesc = &primaryIndex->desc;

		state.name = "primary index";
		state.descr = primaryDesc;
		state.rootdescend = false;
		state.leaf_item_cb = heapallindexed_probe_cb;
		state.leaf_item_arg = &haistate;
		init_page_find_context(&state.context, primaryDesc,
							   COMMITSEQNO_INPROGRESS, BTREE_PAGE_FIND_MODIFY);
		state.npages = 0;

		root_blkno = primaryDesc->rootInfo.rootPageBlkno;
		bounds.depth = 0;
		bounds.leftmost = true;
		clear_fixed_key(&bounds.lokey);
		clear_fixed_key(&bounds.hikey);

		root = palloc(ORIOLEDB_BLCKSZ);
		lock_page(root_blkno);
		change_count = read_locked_page(root, root_blkno);
		unlock_page(root_blkno);

		bounds.level = PAGE_GET_LEVEL(root);
		(void) check_subtree(&state, root_blkno, root, change_count, &bounds, &next_key);
		pfree(root);

		elog(DEBUG1, "heapallindexed: verified " INT64_FORMAT
			 " tuples via primary index, checked %d pages",
			 haistate.heaptuplespresent, state.npages);

		bloom_free(haistate.filter);
	}
}
