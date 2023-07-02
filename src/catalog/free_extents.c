/*-------------------------------------------------------------------------
 *
 * free_extents.c
 * 		Implementation of an orioledb free file extents list.
 *
 * We use two B-trees for holding list of free file extents for indices:
 *
 *	1. Sorted by (datoid, relnode, ixType, extent.offset, extent.length)
 *	- (off, len) as short version in the code.
 *	2. Sorted by (datoid, relnode, ixType, extent.length, extent.offset)
 *	- (len, off) as short version.
 *
 * Pages of this B-trees can be evicted to a disk but a B-tree's state
 * is reset after reboot of the database engine and the state must
 * be restored after it.
 *
 * Copyright (c) 2021-2023, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/catalog/free_extents.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "orioledb.h"

#include "btree/btree.h"
#include "btree/find.h"
#include "btree/io.h"
#include "btree/iterator.h"
#include "btree/merge.h"
#include "btree/modify.h"
#include "btree/page_chunks.h"
#include "catalog/free_extents.h"
#include "tableam/descr.h"
#include "utils/stopevent.h"
#include "utils/page_pool.h"

#include "access/transam.h"
#include "miscadmin.h"
#if PG_VERSION_NUM >= 140000
#include "utils/wait_event.h"
#else
#include "pgstat.h"
#endif

#define EXTENTS_IX_EQ(ex1, ex2) ((ex1).ixType == (ex2).ixType && \
								 (ex1).datoid == (ex2).datoid && \
								 (ex1).relnode == (ex2).relnode)

/*
 * Returns free file extent with length = len.
 *
 * get_extend()/free_extend() operations optimized for more fast get_extend()
 * execution because as more critical for performance part.
 *
 * The main idea of get_extend()/free_extend() B-tree modification:
 * We guarantee successful atomic modification after we fetch a free extent from
 * the (len, off) B-tree.
 *
 * So for successful delete an extent in order (len, off) -> (off, len) we need
 * to insert an extent in (off, len) -> (len, off) order.
 *
 * get_extent() algorithm:
 * 1. Iterate through (len, off) B-tree and delete found extent in-place. For
 * that reason we iterate through the B-tree leafs under lock_page().
 * 2. If found extent is more than needed than return a remaining part into
 * the (off, len) B-tree. It helps do not lost extent on iteration.
 * 3. Delete founded extent from the (off, len) B-tree.
 * 4. If found extent is more than needed than return the remaining part into
 * the (len, off) B-tree.
 */
FileExtent
get_extent(BTreeDescr *desc, uint16 len)
{
	BTreeMetaPage *metaPage = BTREE_GET_META(desc);
	BTreeLeafTuphdr *header;
	FreeTreeTuple tup,
				deleted_tup,
			   *cur_tup;
	FileExtent	result;
	OBTreeFindPageContext context;
	Page		p;
	bool		old_enable_stopevents;
	bool		found = false,
				end = false,
				modify_result;
	BTreePageItemLocator *loc;
	BTreeDescr *len_off_tree = get_sys_tree(SYS_TREES_EXTENTS_LEN_OFF);
	BTreeDescr *off_len_tree = get_sys_tree(SYS_TREES_EXTENTS_OFF_LEN);
	OTuple		tmpTup;

	Assert(!orioledb_s3_mode);

	/* a fast check */
	if (pg_atomic_read_u64(&metaPage->numFreeBlocks) < len)
	{
		/* free extent can not be founded, increase file length */
		result.len = len;
		if (use_device)
			result.off = orioledb_device_alloc(desc, len * ORIOLEDB_COMP_BLCKSZ) / ORIOLEDB_COMP_BLCKSZ;
		else
			result.off = pg_atomic_fetch_add_u64(&metaPage->datafileLength[0], len);
		return result;
	}

	old_enable_stopevents = enable_stopevents;
	enable_stopevents = false;

	tup.ixType = desc->type;
	tup.datoid = desc->oids.datoid;
	tup.relnode = desc->oids.relnode;
	tup.extent.length = len;
	tup.extent.offset = 0;

	init_page_find_context(&context, len_off_tree, COMMITSEQNO_INPROGRESS,
						   BTREE_PAGE_FIND_MODIFY | BTREE_PAGE_FIND_FIX_LEAF_SPLIT);

	/* try to find a free extent */
	while (!found && !end)
	{
		tmpTup.data = (Pointer) &tup;
		tmpTup.formatFlags = 0;
		(void) find_page(&context, (Pointer) &tmpTup, BTreeKeyLeafTuple, 0);
		p = O_GET_IN_MEMORY_PAGE(context.items[context.index].blkno);
		loc = &context.items[context.index].locator;

		while (BTREE_PAGE_LOCATOR_IS_VALID(p, loc))
		{
			BTREE_PAGE_READ_LEAF_ITEM(header, tmpTup, p, loc);
			cur_tup = (FreeTreeTuple *) tmpTup.data;

			if (!EXTENTS_IX_EQ(*cur_tup, tup))
			{
				end = true;
				break;
			}

			if (header->deleted == false)
			{
				found = true;
				break;
			}

			BTREE_PAGE_LOCATOR_NEXT(p, loc);
		}

		if (!found && !end)
		{
			if (O_PAGE_IS(p, RIGHTMOST))
			{
				end = true;
			}
			else
			{
				OTuple		hikey;

				BTREE_PAGE_GET_HIKEY(hikey, p);
				cur_tup = (FreeTreeTuple *) hikey.data;
				if (!EXTENTS_IX_EQ(*cur_tup, tup))
				{
					end = true;
				}
				else
				{
					tup = *cur_tup;
				}
			}
		}

		if (!found)
		{
			unlock_page(context.items[context.index].blkno);
		}
	}

	if (!found)
	{
		/* free extent not founded, increase file length */
		result.len = len;
		if (use_device)
			result.off = orioledb_device_alloc(desc, len * ORIOLEDB_COMP_BLCKSZ) / ORIOLEDB_COMP_BLCKSZ;
		else
			result.off = pg_atomic_fetch_add_u64(&metaPage->datafileLength[0], len);
		enable_stopevents = old_enable_stopevents;
		return result;
	}

	Assert(p != NULL);
	Assert(header != NULL);
	Assert(cur_tup != NULL);
	pg_atomic_fetch_sub_u64(&metaPage->numFreeBlocks, (uint64) len);

	/* delete the extent from the (len, off) B-tree in-place */
	page_block_reads(context.items[context.index].blkno);

	START_CRIT_SECTION();

	header->deleted = true;
	header->xactInfo = OXID_GET_XACT_INFO(BootstrapTransactionId, RowLockUpdate, false);
	PAGE_ADD_N_VACATED(p, BTreeLeafTuphdrSize + sizeof(FreeTreeTuple));

	END_CRIT_SECTION();

	deleted_tup = *cur_tup;

	MARK_DIRTY(len_off_tree->ppool, context.items[context.index].blkno);

	if (is_page_too_sparse(len_off_tree, p))
		(void) btree_try_merge_and_unlock(len_off_tree,
										  context.items[context.index].blkno,
										  false, false);
	else
		unlock_page(context.items[context.index].blkno);

	Assert(deleted_tup.extent.length >= len);
	tup = deleted_tup;
	tup.extent.length -= len;

	if (tup.extent.length > 0)
	{
		/* we have a remaining part, insert it into (off, len) B-tree */
		tup.extent.offset += len;
		tmpTup.formatFlags = 0;
		tmpTup.data = (Pointer) &tup;
		modify_result = o_btree_autonomous_insert(off_len_tree, tmpTup);
		if (!modify_result)
		{
			elog(FATAL, "unable to insert extent (%lu, %lu) into the (off, len) B-tree",
				 tup.extent.offset, tup.extent.length);
		}
	}

	/* delete the extent from the (off, len) B-tree */
	tmpTup.formatFlags = 0;
	tmpTup.data = (Pointer) &deleted_tup;
	modify_result = o_btree_autonomous_delete(off_len_tree, tmpTup, BTreeKeyLeafTuple, NULL);
	if (!modify_result)
	{
		elog(FATAL, "unable to delete extent (%lu, %lu) from the (off, len) B-tree",
			 deleted_tup.extent.offset, deleted_tup.extent.length);
	}

	if (tup.extent.length > 0)
	{
		/*
		 * we have a remaining part, insert it into (len, off) B-tree after
		 * this remaining part may be gotten
		 */
		tmpTup.formatFlags = 0;
		tmpTup.data = (Pointer) &tup;
		modify_result = o_btree_autonomous_insert(len_off_tree, tmpTup);
		if (!modify_result)
		{
			elog(FATAL, "unable to insert extent (%lu, %lu) into the (len, off) B-tree",
				 tup.extent.offset, tup.extent.length);
		}
	}

	result.off = deleted_tup.extent.offset;
	result.len = len;

	enable_stopevents = old_enable_stopevents;
	return result;
}

/*
 * Adds the extent to a free extents list.
 *
 * See description of the get_extent() function.
 *
 * free_extent() algorithm:
 *
 * 1. Find neighbors tuples of the extent in the (off, len) B-tree.
 * 2. Remove neighbors from (len, off) and (off, len) B-trees. If remove from
 * the (len, off) B-tree fails than goto 1.
 * 3. Add merged extent to (off, len) and (len, off) B-trees.
 *
 * TODO: add hints support
 */
void
free_extent(BTreeDescr *desc, FileExtent extent)
{
	BTreeIterator *it = NULL;
	FreeTreeTuple tup,
				right,
				left,
			   *cur;
	bool		old_enable_stopevents = enable_stopevents;
	bool		modify_result,
				merge_right,
				merge_left,
				inserted = false;
	BTreeDescr *len_off_tree = get_sys_tree(SYS_TREES_EXTENTS_LEN_OFF);
	BTreeDescr *off_len_tree = get_sys_tree(SYS_TREES_EXTENTS_OFF_LEN);
	OTuple		tmpTup;

	enable_stopevents = false;

	Assert(FileExtentIsValid(extent));

	memset(&tup, 0, sizeof(FreeTreeTuple));
	memset(&right, 0, sizeof(FreeTreeTuple));
	memset(&left, 0, sizeof(FreeTreeTuple));

	tup.ixType = desc->type;
	tup.datoid = desc->oids.datoid;
	tup.relnode = desc->oids.relnode;

	while (!inserted)
	{
		/* reset status */
		tup.extent.length = 0;
		tup.extent.offset = (uint64) extent.off + extent.len;
		merge_right = false;
		merge_left = false;
		if (it != NULL)
			btree_iterator_free(it);

		/* finds neighbors tuples in the (off, len) B-tree */
		tmpTup.data = (Pointer) &tup;
		tmpTup.formatFlags = 0;
		it = o_btree_iterator_create(off_len_tree, (Pointer) &tmpTup,
									 BTreeKeyNonLeafKey,
									 COMMITSEQNO_INPROGRESS,
									 BackwardScanDirection);
		tmpTup = o_btree_iterator_fetch(it, NULL, NULL, BTreeKeyLeafTuple,
										false, NULL);
		cur = (FreeTreeTuple *) tmpTup.data;
		if (cur == NULL || !EXTENTS_IX_EQ(*cur, tup))
		{
			merge_right = false;
			merge_left = false;
			if (cur != NULL)
				pfree(cur);
			cur = NULL;
		}
		else if (cur->extent.offset != tup.extent.offset)
		{
			Assert(cur->extent.offset < extent.off);
			merge_right = false;
			merge_left = (cur->extent.offset + cur->extent.length) == extent.off;
			if (merge_left)
				left = *cur;
			pfree(cur);
		}
		else
		{
			Assert(cur->extent.offset == tup.extent.offset);
			merge_right = true;
			right = *cur;
			pfree(cur);
			tmpTup = o_btree_iterator_fetch(it, NULL, NULL, BTreeKeyLeafTuple,
											false, NULL);
			cur = (FreeTreeTuple *) tmpTup.data;
			if (cur != NULL && EXTENTS_IX_EQ(*cur, tup)
				&& cur->extent.offset + cur->extent.length == extent.off)
			{
				merge_left = true;
				left = *cur;
			}
			if (cur != NULL)
				pfree(cur);
		}

		/* delete neighbors from the (len, off) B-tree */
		if (merge_right)
		{
			tmpTup.data = (Pointer) &right;
			tmpTup.formatFlags = 0;
			modify_result = o_btree_autonomous_delete(len_off_tree, tmpTup,
													  BTreeKeyLeafTuple, NULL);
			if (!modify_result)
				continue;
		}

		if (merge_left)
		{
			tmpTup.data = (Pointer) &left;
			tmpTup.formatFlags = 0;
			modify_result = o_btree_autonomous_delete(len_off_tree,
													  tmpTup, BTreeKeyLeafTuple, NULL);
			if (!modify_result)
			{
				if (merge_right)
				{
					tmpTup.data = (Pointer) &right;
					tmpTup.formatFlags = 0;
					modify_result = o_btree_autonomous_insert(len_off_tree,
															  tmpTup);
					if (!modify_result)
					{
						elog(FATAL, "unable to return extent (%lu, %lu) into the (len, off) B-tree",
							 right.extent.offset, right.extent.length);
					}
				}
				continue;
			}
		}

		/*
		 * Ok, now delete neighbors from the (off, len) B-tree
		 */
		if (merge_right)
		{
			tmpTup.data = (Pointer) &right;
			tmpTup.formatFlags = 0;
			modify_result = o_btree_autonomous_delete(off_len_tree,
													  tmpTup, BTreeKeyLeafTuple, NULL);
			if (!modify_result)
			{
				elog(FATAL, "unable to delete extent (%lu, %lu) from the (off, len) B-tree",
					 right.extent.offset, right.extent.length);
			}
		}

		if (merge_left)
		{
			tmpTup.data = (Pointer) &left;
			tmpTup.formatFlags = 0;
			modify_result = o_btree_autonomous_delete(off_len_tree,
													  tmpTup, BTreeKeyLeafTuple, NULL);
			if (!modify_result)
			{
				elog(FATAL, "unable to delete extent (%lu, %lu) from the (off, len) B-tree",
					 left.extent.offset, left.extent.length);
			}
		}

		/*
		 * Ok, now insert the merged extent into B-trees.
		 */
		if (merge_left)
		{
			tup.extent.offset = left.extent.offset;
			Assert(tup.extent.offset + left.extent.length == extent.off);
			tup.extent.length = left.extent.length + extent.len;
		}
		else
		{
			tup.extent.offset = extent.off;
			tup.extent.length = extent.len;
		}

		if (merge_right)
		{
			Assert(tup.extent.offset + tup.extent.length == right.extent.offset);
			tup.extent.length += right.extent.length;
		}

		tmpTup.data = (Pointer) &tup;
		tmpTup.formatFlags = 0;
		modify_result = o_btree_autonomous_insert(off_len_tree, tmpTup);
		if (!modify_result)
		{
			elog(FATAL, "unable to insert extent (%lu, %lu) to the (off, len) B-tree",
				 tup.extent.offset, tup.extent.length);
		}

		tmpTup.data = (Pointer) &tup;
		tmpTup.formatFlags = 0;
		modify_result = o_btree_autonomous_insert(len_off_tree, tmpTup);
		if (!modify_result)
		{
			elog(FATAL, "unable to insert extent (%lu, %lu) to the (len, off) B-tree",
				 tup.extent.offset, tup.extent.length);
		}

		inserted = true;
	}
	btree_iterator_free(it);
	enable_stopevents = old_enable_stopevents;
}

/*
 * Calls the callback for each free file extent for a BTree on given csn.
 *
 * Be careful, there are can be some intersections, see get_extent() algorithm.
 */
void
foreach_free_extent(BTreeDescr *desc, ForEachExtentCallback callback, void *arg)
{
	BTreeIterator *it;
	FreeTreeTuple from,
				to,
			   *cur;
	FileExtent	cur_extent;
	bool		old_enable_stopevents = enable_stopevents;
	BTreeDescr *off_len_tree = get_sys_tree(SYS_TREES_EXTENTS_OFF_LEN);
	OTuple		tmpTup;
	OTuple		toTup;
	OTuple		fromTup;

	enable_stopevents = false;

	from.ixType = to.ixType = desc->type;
	from.datoid = to.datoid = desc->oids.datoid;
	from.relnode = to.relnode = desc->oids.relnode;
	from.extent.offset = to.extent.offset = 0;
	from.extent.length = to.extent.length = 0;
	fromTup.data = (Pointer) &from;
	fromTup.formatFlags = 0;

	/* iterate from begin and to the end of the index */
	to.relnode += 1;
	toTup.data = (Pointer) &to;
	toTup.formatFlags = 0;
	Assert(from.relnode < to.relnode);

	it = o_btree_iterator_create(off_len_tree, (Pointer) &fromTup,
								 BTreeKeyNonLeafKey,
								 COMMITSEQNO_INPROGRESS,
								 ForwardScanDirection);

	while (true)
	{
		tmpTup = o_btree_iterator_fetch(it, NULL, &toTup,
										BTreeKeyNonLeafKey, false, NULL);
		if (O_TUPLE_IS_NULL(tmpTup))
			break;
		cur = (FreeTreeTuple *) tmpTup.data;

		Assert(cur->ixType == desc->type);
		Assert(cur->datoid == desc->oids.datoid);
		Assert(cur->relnode == desc->oids.relnode);

		/* FreeTreeFileExtent.length may be more than FileExtent.len */
		while (cur->extent.length > UINT16_MAX)
		{
			cur_extent.off = cur->extent.offset;
			cur_extent.len = UINT16_MAX;
			callback(desc, cur_extent, arg);
			cur->extent.offset += UINT16_MAX;
			cur->extent.length -= UINT16_MAX;
		}
		cur_extent.off = cur->extent.offset;
		cur_extent.len = cur->extent.length;
		callback(desc, cur_extent, arg);
		pfree(cur);
	}

	btree_iterator_free(it);
	enable_stopevents = old_enable_stopevents;
}

/*
 * Adds free extents from .tmp file to the trees.  Optionally removes the .tmp
 * file.
 */
void
add_free_extents_from_tmp(BTreeDescr *desc, bool remove)
{
	BTreeMetaPage *metaPage;
	File		file;
	char	   *filename,
				buf[ORIOLEDB_BLCKSZ];
	uint64		len = 0,
				i,
				buf_len;
	uint32		chkp_num;
	LWLock	   *metaLock;

	o_btree_load_shmem(desc);
	metaPage = BTREE_GET_META(desc);
	metaLock = &metaPage->metaLock;

	chkp_num = metaPage->freeBuf.tag.num + 1;
	if (!can_use_checkpoint_extents(desc, metaPage->freeBuf.tag.num + 1))
		return;

	LWLockAcquire(metaLock, LW_EXCLUSIVE);
	chkp_num = metaPage->freeBuf.tag.num + 1;
	while (can_use_checkpoint_extents(desc, chkp_num))
	{
		metaPage->freeBuf.tag.num = chkp_num;
		metaPage->freeBuf.tag.type = 't';
		if (!seq_buf_file_exist(&metaPage->freeBuf.tag))
		{
			/* table may be deleted or *.tmp file not created */
			chkp_num++;
			continue;
		}

		/* free extents from *.tmp file */
		filename = get_seq_buf_filename(&metaPage->freeBuf.tag);
		file = PathNameOpenFile(filename, O_RDONLY | PG_BINARY);
		if (file < 0)
			ereport(FATAL, (errcode_for_file_access(),
							errmsg("could not open file %s", filename)));

		do
		{
			FileExtent *cur_off;

			buf_len = OFileRead(file, buf, ORIOLEDB_BLCKSZ, len, WAIT_EVENT_DATA_FILE_READ);
			cur_off = (FileExtent *) buf;
			for (i = 0; i < buf_len; i += sizeof(FileExtent))
			{
				pg_atomic_fetch_add_u64(&metaPage->numFreeBlocks,
										(uint64) cur_off->len);
				free_extent(desc, *cur_off);
				cur_off++;
			}
			len += buf_len;
		}
		while (buf_len == ORIOLEDB_BLCKSZ);

		pfree(filename);
		FileClose(file);

		if (remove_old_checkpoint_files)
			seq_buf_remove_file(&metaPage->freeBuf.tag);
	}
	LWLockRelease(metaLock);
}
