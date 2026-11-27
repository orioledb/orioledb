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
#include "btree/scan.h"
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
#include "storage/latch.h"

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

/* How many times check_btree() looks again while extents keep moving */
#define CHECK_EXTENTS_MAX_ATTEMPTS 100

static int	file_extent_cmp(const void *p1, const void *p2);
static void check_walk_btree(BTreeCheckStatus *status, OInMemoryBlkno blkno,
							 OInMemoryBlkno parentPagenum);
static void add_extent(ExtentsArray *arr, FileExtent extent);
static bool check_extents(ExtentsArray *busy, ExtentsArray *free,
						  bool report);
static bool check_file_length(uint64 data_file_len, ExtentsArray *busy,
							  ExtentsArray *free, bool report);
static void get_free_extents(BTreeDescr *desc, ExtentsArray *free_extents,
							 uint32 chkp_num);
static bool check_checkpoint_image(BTreeDescr *desc);
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

bool
check_btree(BTreeDescr *desc, bool force_file_check, bool wait_for_checkpoint)
{
	bool		is_sys_tree = IS_SYS_TREE_OIDS(desc->oids);
	BTreeMetaPage *metaPageBlkno = BTREE_GET_META(desc);
	BTreeCheckStatus status;
	ExtentsArray free_extents;
	bool		is_compressed = OCompressIsValid(desc->compress);
	uint32		checkpoint_number = 0;
	bool		copy_blkno;
	int			attempt;

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

	/*
	 * The tree's pages and its free stream are read one after the other, and
	 * page writes do not wait for us: a page rewritten in between is seen at
	 * its old extent, which the stream already lists as free, while its new
	 * extent is gone from the stream.  So a mismatch only counts if no extent
	 * moved while we looked; otherwise look again.
	 */
	for (attempt = 1;; attempt++)
	{
		uint64		changes = pg_atomic_read_u64(&metaPageBlkno->extentsChangeCount);
		uint64		data_file_len;
		bool		matches;

		init_page_find_context(&status.context, desc, COMMITSEQNO_INPROGRESS, BTREE_PAGE_FIND_MODIFY);

		/*
		 * get_checkpoint_number() returns false while the checkpointer holds
		 * the per-tree state.  With wait_for_checkpoint, retry until it moves
		 * past; otherwise the caller is a debug entry point and gets the
		 * legacy NOTICE.
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
			 * Holding the per-tree lock here would block the checkpointer we
			 * are waiting on, so drop it across the latch wait and reacquire
			 * after. Sys trees serialize against the checkpointer via
			 * oSysTreesLock.
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

		/* Fix for S3 mode */
		data_file_len = pg_atomic_read_u64(&metaPageBlkno->datafileLength[0]);

		check_walk_btree(&status, desc->rootInfo.rootPageBlkno, OInvalidInMemoryBlkno);

		if (status.hasError)
			return false;

		if (desc->storageType != BTreeStoragePersistence)
			return true;

		/* get free file extents */
		get_free_extents(desc, &free_extents, checkpoint_number - 1);

		matches = check_extents(&status.busy, &free_extents, false) &&
			check_file_length(data_file_len, &status.busy, &free_extents, false);
		if (matches)
			break;

		if (pg_atomic_read_u64(&metaPageBlkno->extentsChangeCount) == changes ||
			attempt >= CHECK_EXTENTS_MAX_ATTEMPTS)
		{
			/* Again, to say what does not match */
			(void) check_extents(&status.busy, &free_extents, true);
			(void) check_file_length(data_file_len, &status.busy, &free_extents, true);
			status.hasError = true;
			break;
		}

		status.busy.size = status.busy.blocksCount = 0;
		free_extents.size = free_extents.blocksCount = 0;
		(void) WaitLatch(MyLatch,
						 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						 10L, WAIT_EVENT_PG_SLEEP);
		ResetLatch(MyLatch);
		CHECK_FOR_INTERRUPTS();
	}

	/* frees allocated bytes */
	if (status.busy.extents != NULL)
		pfree(status.busy.extents);
	if (free_extents.extents != NULL)
		pfree(free_extents.extents);

	if (status.hasError)
		return false;

	/*
	 * Validate the checkpoint image separately; it can be older than the live
	 * tree checked above.
	 */
	if (force_file_check && !orioledb_s3_mode &&
		!check_checkpoint_image(desc))
		status.hasError = true;

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
 * Gets the extents the tree's free stream still holds: what the free buffer
 * has yet to read, and what is freed for the coming checkpoints.
 */
static void
get_free_extents(BTreeDescr *desc, ExtentsArray *free_extents, uint32 chkp_num)
{
	SeqBufTag	chkp_tag;
	bool		is_compressed = OCompressIsValid(desc->compress);

	chkp_tag.key.oids = desc->oids;
	chkp_tag.key.oids.spcoid = desc->oids.spcoid;

	if (!is_compressed)
	{
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
 * Appends the extents of the checkpoint image pages under the downlink.
 */
static bool
get_image_extents(BTreeDescr *desc, uint64 downlink, ExtentsArray *busy)
{
	char		pg_attribute_aligned(sizeof(uint32)) buf[ORIOLEDB_BLCKSZ];
	Page		p = (Page) buf;
	FileExtent	extent;
	BTreePageItemLocator loc;
	bool		result = true;

	check_stack_depth();
	CHECK_FOR_INTERRUPTS();

	if (!FileExtentLenIsValid(DOWNLINK_GET_DISK_LEN(downlink)))
	{
		elog(NOTICE, "Checkpoint image downlink %lu %u has an invalid length",
			 (unsigned long) DOWNLINK_GET_DISK_OFF(downlink),
			 (unsigned) DOWNLINK_GET_DISK_LEN(downlink));
		return false;
	}

	if (read_page_from_disk(desc, buf, downlink, &extent) != OReadPageResultOk)
	{
		elog(NOTICE, "Could not read checkpoint image page %lu %u",
			 (unsigned long) extent.off, (unsigned) extent.len);
		return false;
	}
	add_extent(busy, extent);

	if (O_PAGE_IS(p, LEAF))
		return true;

	BTREE_PAGE_FOREACH_ITEMS(p, &loc)
	{
		BTreeNonLeafTuphdr *tuphdr = (BTreeNonLeafTuphdr *) BTREE_PAGE_LOCATOR_GET_ITEM(p, &loc);

		if (!DOWNLINK_IS_ON_DISK(tuphdr->downlink))
		{
			elog(NOTICE, "Checkpoint image page %lu %u has a downlink not on disk",
				 (unsigned long) extent.off, (unsigned) extent.len);
			result = false;
			continue;
		}
		if (!get_image_extents(desc, tuphdr->downlink, busy))
			result = false;
	}
	return result;
}

/*
 * Validates the last checkpoint's on-disk state: the image pages and the free
 * extents listed in the map file must cover the data file up to the map's
 * datafileLength exactly once.  That image can be older than the live tree;
 * the check verifies that recovery would start from a self-consistent
 * snapshot.
 *
 * The map is the free space of that image, not of the live tree: pages
 * written since have taken extents it lists, and the live free stream lists
 * them in another order.  The image itself stays put while we hold it.
 */
static bool
check_checkpoint_image(BTreeDescr *desc)
{
	BTreeSeqScan *hold;
	uint32		chkp_num;
	SeqBufTag	tag;
	CheckpointFileHeader header;
	ExtentsArray image,
				map;
	bool		found;
	bool		result = true;
	char	   *filename;
	File		file;

	memset(&image, 0, sizeof(ExtentsArray));
	memset(&map, 0, sizeof(ExtentsArray));
	memset(&tag, 0, sizeof(SeqBufTag));

	/*
	 * Which map describes the image is what the checkpoints recorded, not
	 * what files exist: a map file a checkpoint has not finished has its
	 * header unwritten, and whatever the seq buf page held there.  A tree
	 * loaded after checkpoint N gets its map N + 1 at once, and keeps it so
	 * if checkpoint N + 1 skips the tree.  The checkpoint that wrote the tree
	 * records that after marking the tree done, so wait for a checkpoint that
	 * has just written it to finish.
	 */
	while (true)
	{
		hold = btree_hold_checkpoint_image(desc, &chkp_num);
		pg_read_barrier();
		if (chkp_num <= checkpoint_state->lastCheckpointNumber)
			break;
		free_btree_seq_scan(hold);
		(void) WaitLatch(MyLatch,
						 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						 10L, WAIT_EVENT_PG_SLEEP);
		ResetLatch(MyLatch);
		CHECK_FOR_INTERRUPTS();
	}

	tag.key.oids = desc->oids;
	tag.type = 'm';
	tag.num = o_get_latest_chkp_num(desc->oids.datoid,
									desc->oids.relnode,
									desc->oids.spcoid,
									chkp_num, &found);

	filename = get_seq_buf_filename(&tag);
	file = PathNameOpenFile(filename, O_RDONLY | PG_BINARY);
	if (file < 0)
	{
		/* A tree no checkpoint has written yet has no map */
		if (found)
		{
			elog(NOTICE, "%s does not exist", filename);
			result = false;
		}
		pfree(filename);
		free_btree_seq_scan(hold);
		return result;
	}

	if (OFileRead(file, (Pointer) &header, sizeof(header), 0,
				  WAIT_EVENT_DATA_FILE_READ) != sizeof(header))
	{
		elog(NOTICE, "Could not read the header of %s", filename);
		FileClose(file);
		pfree(filename);
		free_btree_seq_scan(hold);
		return false;
	}
	FileClose(file);

	if (!found)
	{
		/* Not written by a checkpoint: what is there is no header */
		pfree(filename);
		free_btree_seq_scan(hold);
		return true;
	}

	if (DiskDownlinkIsValid(header.rootDownlink) &&
		!FileExtentLenIsValid(DOWNLINK_GET_DISK_LEN(header.rootDownlink)))
	{
		elog(NOTICE, "%s has an invalid root downlink", filename);
		pfree(filename);
		free_btree_seq_scan(hold);
		return false;
	}
	get_free_extents_from_file(&tag, sizeof(CheckpointFileHeader), &map,
							   OCompressIsValid(desc->compress), true);
	if (header.numFreeBlocks != map.size)
	{
		elog(NOTICE, "%s header counts " UINT64_FORMAT " free extents, the file has " UINT64_FORMAT,
			 filename, header.numFreeBlocks, map.size);
		result = false;
	}

	if (DiskDownlinkIsValid(header.rootDownlink) &&
		!get_image_extents(desc, header.rootDownlink, &image))
		result = false;

	if (result &&
		!(check_extents(&image, &map, true) &&
		  check_file_length(header.datafileLength, &image, &map, true)))
	{
		elog(NOTICE, "%s does not match the image of checkpoint %u",
			 filename, tag.num);
		result = false;
	}

	free_btree_seq_scan(hold);
	pfree(filename);
	if (image.extents)
		pfree(image.extents);
	if (map.extents)
		pfree(map.extents);
	return result;
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
 * Returns true if the busy and free extents together are as long as the
 * data file.
 */
static bool
check_file_length(uint64 data_file_len, ExtentsArray *busy, ExtentsArray *free,
				  bool report)
{
	uint64		len = busy->blocksCount + free->blocksCount;

	if (data_file_len > len)
	{
		if (report)
			elog(NOTICE, "Not used file blocks from " UINT64_FORMAT " to " UINT64_FORMAT,
				 len, data_file_len);
		return false;
	}
	if (data_file_len < len)
	{
		if (report)
			elog(NOTICE, "Excess file blocks from " UINT64_FORMAT " to " UINT64_FORMAT,
				 data_file_len, len);
		return false;
	}
	return true;
}

/*
 * Returns true if the busy and free extents array do not intersect and have no
 * holes.
 */
static bool
check_extents(ExtentsArray *busy, ExtentsArray *free, bool report)
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
			if (report)
				elog(NOTICE, "Excess busy extent %lu %u",
					 (unsigned long) busy->extents[b].off,
					 (unsigned) busy->extents[b].len);
			result = false;
			b++;
		}

		while (f < free->size && next_off > free->extents[f].off)
		{
			if (report)
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
				if (report)
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
				if (report)
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
