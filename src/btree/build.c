/*-------------------------------------------------------------------------
 *
 * build.c
 *		Routines for sort-based B-tree index building.
 *
 * Copyright (c) 2021-2023, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/btree/build.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "orioledb.h"

#include "btree/build.h"
#include "btree/insert.h"
#include "btree/io.h"
#include "btree/page_chunks.h"
#include "btree/split.h"
#include "checkpoint/checkpoint.h"
#include "recovery/recovery.h"
#include "tableam/descr.h"
#include "tuple/toast.h"
#include "tuple/sort.h"
#include "transam/oxid.h"
#include "utils/seq_buf.h"

#include "access/genam.h"
#include "access/relation.h"
#include "catalog/index.h"
#include "catalog/namespace.h"
#include "commands/defrem.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "utils/lsyscache.h"
#include "utils/memdebug.h"

typedef struct OIndexBuildStackItem
{
	char		img[ORIOLEDB_BLCKSZ];
	BTreePageItemLocator loc;
	OFixedKey	key;
	int			keysize;
} OIndexBuildStackItem;

static bool put_item_to_stack(BTreeDescr *desc, OIndexBuildStackItem *stack,
							  int level, OTuple tuple, int tuplesize,
							  Pointer tupleheader, LocationIndex header_size,
							  int *root_level, BTreeMetaPage *metaPageBlkno);
static bool put_tuple_to_stack(BTreeDescr *desc, OIndexBuildStackItem *stack,
							   OTuple tuple, int *root_level,
							   BTreeMetaPage *metaPageBlkno);
static bool put_downlink_to_stack(BTreeDescr *desc, OIndexBuildStackItem *stack,
								  int level, uint64 downlink, OTuple key,
								  int keysize, int *root_level,
								  BTreeMetaPage *metaPageBlkno);

static void
stack_page_split(BTreeDescr *desc, OIndexBuildStackItem *stack, int level,
				 OTuple tuple, int tuplesize, Pointer tupleheader,
				 LocationIndex header_size, Page new_page)
{
	Page		img = stack[level].img;
	OffsetNumber left_count,
				rightbound_key_size;
	bool		key_palloc = false;
	Pointer		tuple_ptr;
	OTuple		rightbound_key;
	bool		leaf = O_PAGE_IS(img, LEAF);
	int			i;
	BTreePageItemLocator loc,
				newLoc;
	BTreePageItem items[BTREE_PAGE_MAX_CHUNK_ITEMS];
	OffsetNumber offset;

	btree_page_update_max_key_len(desc, img);
	offset = BTREE_PAGE_LOCATOR_GET_OFFSET(img, &stack[level].loc);
	left_count = btree_page_split_location(desc, img, offset,
										   tuplesize, tuple, false, offset,
										   0.9, NULL, COMMITSEQNO_INPROGRESS);


	/* Distribute the tuples according the the split location */
	i = 0;
	BTREE_PAGE_LOCATOR_FIRST(img, &loc);
	while (i < left_count)
	{
		Assert(BTREE_PAGE_LOCATOR_IS_VALID(img, &loc));

		/*
		 * In leaf pages, get rid of tuples deleted by finished transactions.
		 * Also, resize tuples to minimal size.  In non-leaf pages, copy
		 * tuples as-is.
		 */
		if (leaf)
		{
			BTreeLeafTuphdr *tupHdr;
			OTuple		tup;
			bool		finished;

			BTREE_PAGE_READ_LEAF_ITEM(tupHdr, tup, img, &loc);
			finished = XACT_INFO_FINISHED_FOR_EVERYBODY(tupHdr->xactInfo);
			if (finished && tupHdr->deleted != BTreeLeafTupleNonDeleted)
			{
				left_count--;
				continue;
			}
			items[i].data = (Pointer) tupHdr;
			items[i].flags = tup.formatFlags;
			items[i].size = finished ?
				(BTreeLeafTuphdrSize + MAXALIGN(o_btree_len(desc, tup, OTupleLength))) :
				BTREE_PAGE_GET_ITEM_SIZE(img, &loc);
			items[i].newItem = false;
		}
		else
		{
			items[i].data = BTREE_PAGE_LOCATOR_GET_ITEM(img, &loc);
			items[i].flags = BTREE_PAGE_GET_ITEM_FLAGS(img, &loc);
			items[i].size = BTREE_PAGE_GET_ITEM_SIZE(img, &loc);
		}
		items[i].newItem = false;
		i++;
		BTREE_PAGE_LOCATOR_NEXT(img, &loc);
	}

	BTREE_PAGE_LOCATOR_FIRST(new_page, &newLoc);
	while (BTREE_PAGE_LOCATOR_IS_VALID(img, &loc))
	{
		LocationIndex itemsize;

		itemsize = BTREE_PAGE_GET_ITEM_SIZE(img, &loc);

		page_locator_insert_item(new_page, &newLoc, itemsize);
		memcpy(BTREE_PAGE_LOCATOR_GET_ITEM(new_page, &newLoc),
			   BTREE_PAGE_LOCATOR_GET_ITEM(img, &loc),
			   itemsize);
		BTREE_PAGE_SET_ITEM_FLAGS(new_page, &newLoc, BTREE_PAGE_GET_ITEM_FLAGS(stack[level].img, &loc));

		BTREE_PAGE_LOCATOR_NEXT(img, &loc);
		BTREE_PAGE_LOCATOR_NEXT(new_page, &newLoc);
	}

	BTREE_PAGE_LOCATOR_TAIL(new_page, &newLoc);
	page_locator_insert_item(new_page, &newLoc,
							 MAXALIGN(tuplesize) + header_size);
	tuple_ptr = BTREE_PAGE_LOCATOR_GET_ITEM(new_page, &newLoc);
	memcpy(tuple_ptr, tupleheader, header_size);
	tuple_ptr += header_size;
	memcpy(tuple_ptr, tuple.data, tuplesize);
	BTREE_PAGE_SET_ITEM_FLAGS(new_page, &newLoc, tuple.formatFlags);

	/* Setup the new high key on the left page */
	BTREE_PAGE_LOCATOR_FIRST(new_page, &newLoc);
	BTREE_PAGE_READ_TUPLE(rightbound_key, new_page, &newLoc);
	if (leaf)
	{
		rightbound_key = o_btree_tuple_make_key(desc, rightbound_key, NULL, false, &key_palloc);
		rightbound_key_size = o_btree_len(desc, rightbound_key, OKeyLength);
	}
	else
	{
		rightbound_key_size = BTREE_PAGE_GET_ITEM_SIZE(new_page, &newLoc) -
			header_size;
	}

	btree_page_reorg(desc, img, items, left_count,
					 rightbound_key_size, rightbound_key, NULL);

	if (key_palloc)
		pfree(rightbound_key.data);
}

static bool
put_item_to_stack(BTreeDescr *desc, OIndexBuildStackItem *stack, int level,
				  OTuple tuple, int tuplesize, Pointer tupleheader,
				  LocationIndex header_size, int *root_level,
				  BTreeMetaPage *metaPageBlkno)
{
	BTreeItemPageFitType fit;
	Pointer		tuple_ptr;
	uint64		downlink = 0;

	Assert(level < ORIOLEDB_MAX_DEPTH);

	fit = page_locator_fits_item(desc,
								 stack[level].img,
								 &stack[level].loc,
								 MAXALIGN(tuplesize) + header_size,
								 false,
								 COMMITSEQNO_INPROGRESS);

	if (fit == BTreeItemPageFitAsIs)
	{
		page_locator_insert_item(stack[level].img, &stack[level].loc,
								 MAXALIGN(tuplesize) + header_size);
		tuple_ptr = BTREE_PAGE_LOCATOR_GET_ITEM(stack[level].img, &stack[level].loc);
		memcpy(tuple_ptr, tupleheader, header_size);
		tuple_ptr += header_size;
		memcpy(tuple_ptr, tuple.data, tuplesize);
		BTREE_PAGE_SET_ITEM_FLAGS(stack[level].img, &stack[level].loc, tuple.formatFlags);

		BTREE_PAGE_LOCATOR_NEXT(stack[level].img, &stack[level].loc);
	}
	else
	{
		FileExtent	extent;
		char		new_page[ORIOLEDB_BLCKSZ] = {0};
		OFixedKey	key;
		int			keysize;
		BTreePageHeader *new_page_header = (BTreePageHeader *) new_page;
		BTreePageHeader *header = (BTreePageHeader *) stack[level].img;
		BTreePageHeader *parent_header = (BTreePageHeader *) stack[level + 1].img;

		new_page_header->rightLink = InvalidRightLink;
		new_page_header->csn = COMMITSEQNO_FROZEN;
		new_page_header->undoLocation = InvalidUndoLocation;
		new_page_header->checkpointNum = 0;
		new_page_header->prevInsertOffset = MaxOffsetNumber;

		new_page_header->flags = O_BTREE_FLAG_RIGHTMOST;

		if (level == 0)
			new_page_header->flags |= O_BTREE_FLAG_LEAF;
		else
			PAGE_SET_LEVEL(new_page, level);

		init_page_first_chunk(desc, new_page, 0);

		header->rightLink = InvalidRightLink;
		header->csn = COMMITSEQNO_FROZEN;
		header->undoLocation = InvalidUndoLocation;
		header->checkpointNum = 0;
		header->prevInsertOffset = MaxOffsetNumber;

		header->flags &= ~O_BTREE_FLAG_RIGHTMOST;

		if (level == 0)
			header->flags |= O_BTREE_FLAG_LEAF;

		stack_page_split(desc, stack, level, tuple, tuplesize, tupleheader,
						 header_size, new_page);

		if (level == *root_level)
		{
			parent_header->flags = O_BTREE_FLAG_RIGHTMOST | O_BTREE_FLAG_LEFTMOST;
			header->flags |= O_BTREE_FLAG_LEFTMOST;
			if (level != 0)
				PAGE_SET_LEVEL(stack[level].img, level);

			*root_level = level + 1;
		}

		if (level != 0)
			PAGE_SET_N_ONDISK(stack[level].img,
							  BTREE_PAGE_ITEMS_COUNT(stack[level].img));

		/* write old page to disk */

		extent.len = InvalidFileExtentLen;
		extent.off = InvalidFileExtentOff;

		VALGRIND_CHECK_MEM_IS_DEFINED(stack[level].img, ORIOLEDB_BLCKSZ);

		downlink = perform_page_io_build(desc, stack[level].img, &extent, metaPageBlkno);
		if (level == 0)
			pg_atomic_add_fetch_u32(&metaPageBlkno->leafPagesNum, 1);

		copy_fixed_key(desc, &key, stack[level].key.tuple);
		keysize = stack[level].keysize;

		stack[level].keysize = BTREE_PAGE_GET_HIKEY_SIZE(stack[level].img);
		copy_fixed_hikey(desc, &stack[level].key, stack[level].img);

		if (level > 0)
		{
#ifdef ORIOLEDB_CUT_FIRST_KEY
			page_cut_first_key(new_page);
#endif
		}

		/* copy new page to stack */
		memcpy(stack[level].img, new_page, ORIOLEDB_BLCKSZ);
		BTREE_PAGE_LOCATOR_TAIL(stack[level].img, &stack[level].loc);

		put_downlink_to_stack(desc, stack, level + 1, downlink,
							  key.tuple, keysize,
							  root_level, metaPageBlkno);
	}
	return true;
}

static bool
put_downlink_to_stack(BTreeDescr *desc, OIndexBuildStackItem *stack, int level,
					  uint64 downlink, OTuple key, int keysize,
					  int *root_level, BTreeMetaPage *metaPageBlkno)
{
	BTreeNonLeafTuphdr internal_header = {0};
	bool		result;

	internal_header.downlink = downlink;
	result = put_item_to_stack(desc, stack, level, key, keysize,
							   (Pointer) &internal_header,
							   sizeof(internal_header), root_level,
							   metaPageBlkno);
	return result;
}

static bool
put_tuple_to_stack(BTreeDescr *desc, OIndexBuildStackItem *stack,
				   OTuple tuple, int *root_level, BTreeMetaPage *metaPageBlkno)
{
	BTreeLeafTuphdr leaf_header = {0};
	int			tuplesize;

	leaf_header.deleted = BTreeLeafTupleNonDeleted;
	leaf_header.undoLocation = InvalidUndoLocation;
	leaf_header.xactInfo = OXID_GET_XACT_INFO(BootstrapTransactionId, RowLockUpdate, false);
	tuplesize = o_btree_len(desc, tuple, OTupleLength);
	return put_item_to_stack(desc, stack, 0,
							 tuple, tuplesize, (Pointer) &leaf_header,
							 sizeof(leaf_header), root_level, metaPageBlkno);
}

void
btree_write_index_data(BTreeDescr *desc, TupleDesc tupdesc,
					   Tuplesortstate *sortstate, uint64 ctid,
					   CheckpointFileHeader *file_header)
{
	OTuple		idx_tup;
	OIndexBuildStackItem *stack;
	int			root_level = 0,
				saved_root_level;
	Page		root_page;
	uint64		downlink;
	BTreePageHeader *root_page_header;
	FileExtent	extent;
	BTreeMetaPage metaPageBlkno = {0};
	int			i;
	Datum	   *values;
	bool	   *isnull;
	uint32		chkpNum;

	btree_open_smgr(desc);

	stack = (OIndexBuildStackItem *) palloc0(sizeof(OIndexBuildStackItem) * ORIOLEDB_MAX_DEPTH);
	values = (Datum *) palloc(sizeof(Datum) * tupdesc->natts);
	isnull = (bool *) palloc(sizeof(bool) * tupdesc->natts);

	pg_atomic_init_u64(&metaPageBlkno.datafileLength[0], 0);
	pg_atomic_init_u64(&metaPageBlkno.datafileLength[1], 0);
	pg_atomic_init_u64(&metaPageBlkno.numFreeBlocks, 0);
	pg_atomic_init_u32(&metaPageBlkno.leafPagesNum, 0);
	pg_atomic_init_u64(&metaPageBlkno.ctid, ctid);
	for (i = 0; i < ORIOLEDB_MAX_DEPTH; i++)
	{
		/* init_page_first_chunk() needs leaf flag to be set */
		if (i == 0)
			((BTreePageHeader *) stack[i].img)->flags = O_BTREE_FLAG_LEAF;
		init_page_first_chunk(desc, stack[i].img, 0);
		BTREE_PAGE_LOCATOR_FIRST(stack[i].img, &stack[i].loc);
	}

	idx_tup = tuplesort_getotuple(sortstate, true);
	while (!O_TUPLE_IS_NULL(idx_tup))
	{
		Assert(o_tuple_size(idx_tup, &((OIndexDescr *) desc->arg)->leafSpec) <= O_BTREE_MAX_TUPLE_SIZE);
		put_tuple_to_stack(desc, stack, idx_tup, &root_level, &metaPageBlkno);
		idx_tup = tuplesort_getotuple(sortstate, true);
	}

	pfree(values);
	pfree(isnull);

	saved_root_level = root_level;
	for (i = 0; i < saved_root_level; i++)
	{
		if (i != 0)
			PAGE_SET_N_ONDISK(stack[i].img, BTREE_PAGE_ITEMS_COUNT(stack[i].img));

		extent.len = InvalidFileExtentLen;
		extent.off = InvalidFileExtentOff;

		VALGRIND_CHECK_MEM_IS_DEFINED(stack[i].img, ORIOLEDB_BLCKSZ);

		split_page_by_chunks(desc, stack[i].img);
		downlink = perform_page_io_build(desc, stack[i].img, &extent, &metaPageBlkno);
		if (i == 0)
			pg_atomic_add_fetch_u32(&metaPageBlkno.leafPagesNum, 1);

		put_downlink_to_stack(desc, stack, i + 1, downlink,
							  stack[i].key.tuple, stack[i].keysize,
							  &root_level, &metaPageBlkno);
	}

	root_page = stack[root_level].img;

	root_page_header = (BTreePageHeader *) root_page;
	if (root_level == 0)
		root_page_header->flags = O_BTREE_FLAGS_ROOT_INIT;
	root_page_header->rightLink = InvalidRightLink;
	root_page_header->csn = COMMITSEQNO_FROZEN;
	root_page_header->undoLocation = InvalidUndoLocation;
	root_page_header->checkpointNum = 0;
	root_page_header->prevInsertOffset = MaxOffsetNumber;

	if (!O_PAGE_IS(root_page, LEAF))
	{
		PAGE_SET_N_ONDISK(root_page, BTREE_PAGE_ITEMS_COUNT(root_page));
		PAGE_SET_LEVEL(root_page, root_level);
	}

	extent.len = InvalidFileExtentLen;
	extent.off = InvalidFileExtentOff;

	VALGRIND_CHECK_MEM_IS_DEFINED(root_page, ORIOLEDB_BLCKSZ);

	split_page_by_chunks(desc, root_page);
	downlink = perform_page_io_build(desc, root_page, &extent, &metaPageBlkno);
	if (root_level == 0)
		pg_atomic_add_fetch_u32(&metaPageBlkno.leafPagesNum, 1);

	btree_close_smgr(desc);
	pfree(stack);

	if (orioledb_s3_mode)
		chkpNum = S3_GET_CHKP_NUM(DOWNLINK_GET_DISK_OFF(downlink));
	else
		chkpNum = 0;

	memset(file_header, 0, sizeof(*file_header));
	file_header->rootDownlink = downlink;
	file_header->datafileLength = pg_atomic_read_u64(&metaPageBlkno.datafileLength[chkpNum % 2]);
	file_header->numFreeBlocks = pg_atomic_read_u64(&metaPageBlkno.numFreeBlocks);
	file_header->leafPagesNum = pg_atomic_read_u32(&metaPageBlkno.leafPagesNum);
	file_header->ctid = pg_atomic_read_u64(&metaPageBlkno.ctid);
}

void
btree_write_file_header(BTreeDescr *desc, CheckpointFileHeader *file_header)
{
	File		file;
	uint32		checkpoint_number;
	bool		checkpoint_concurrent;
	char	   *filename;

	Assert(desc->storageType == BTreeStoragePersistence ||
		   desc->storageType == BTreeStorageTemporary);

	checkpoint_number = get_cur_checkpoint_number(&desc->oids, desc->type,
												  &checkpoint_concurrent);

	if (desc->storageType == BTreeStoragePersistence)
	{
		SeqBufTag	prev_chkp_tag;

		memset(&prev_chkp_tag, 0, sizeof(prev_chkp_tag));
		prev_chkp_tag.datoid = desc->oids.datoid;
		prev_chkp_tag.relnode = desc->oids.relnode;
		prev_chkp_tag.num = checkpoint_number;
		prev_chkp_tag.type = 'm';

		filename = get_seq_buf_filename(&prev_chkp_tag);

		file = PathNameOpenFile(filename, O_WRONLY | O_CREAT | PG_BINARY);

		if (OFileWrite(file, (Pointer) file_header,
					   sizeof(CheckpointFileHeader), 0,
					   WAIT_EVENT_DATA_FILE_WRITE) !=
			sizeof(CheckpointFileHeader))
		{
			pfree(filename);
			ereport(FATAL,
					(errcode_for_file_access(),
					 errmsg("Could not write checkpoint header to file: %s",
							filename)));
		}
	}
	else
	{
		EvictedTreeData evicted_tree_data = {{0}};

		evicted_tree_data.file_header = *file_header;
		filename = get_eviction_filename(desc->oids, checkpoint_number + 1);

		file = PathNameOpenFile(filename, O_WRONLY | O_CREAT | PG_BINARY);
		if (file < 0)
		{
			pfree(filename);
			ereport(FATAL,
					(errcode_for_file_access(),
					 errmsg("Could not open eviction file: %s", filename)));
		}

		if (OFileWrite(file, (Pointer) &evicted_tree_data,
					   sizeof(evicted_tree_data), 0,
					   WAIT_EVENT_DATA_FILE_WRITE) != sizeof(evicted_tree_data))
		{
			FileClose(file);
			pfree(filename);
			ereport(FATAL, (errcode_for_file_access(),
							errmsg("Could not write eviction data to file: %s",
								   filename)));
		}
	}
	FileClose(file);
	pfree(filename);
}
