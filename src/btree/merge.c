/*-------------------------------------------------------------------------
 *
 * merge.c
 *		Routines for implementation of B-tree pages merge.
 *
 * Copyright (c) 2021-2022, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/btree/merge.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "orioledb.h"

#include "btree/find.h"
#include "btree/io.h"
#include "btree/merge.h"
#include "btree/page_chunks.h"
#include "btree/undo.h"
#include "checkpoint/checkpoint.h"
#include "utils/page_pool.h"
#include "transam/undo.h"

#include "miscadmin.h"

/*
 * If the ratio of free to total space on a leaf page is greater than the value
 * then we will try to merge the node page.
 */
#define O_MERGE_LEAF_FREE_RATIO (0.7)
/*
 * If the ratio of free to total space on a node page is greater than the value
 * then we will try to merge the node page.
 */
#define O_MERGE_NODE_FREE_RATIO (0.7)

static bool can_be_merged(BTreeDescr *desc, Page left, Page right,
						  CommitSeqNo csn);
static void merge_pages(BTreeDescr *desc, OInMemoryBlkno left_blkno,
						Page right, CommitSeqNo csn);


/*
 * Try to merge right page to the left page.  Returns true iff succeed.
 *
 * On success, all pages are unlocked.  On failure, all locks are held.
 */
bool
btree_try_merge_pages(BTreeDescr *desc,
					  OInMemoryBlkno parent_blkno, OFixedKey *parent_hikey,
					  bool *merge_parent,
					  OInMemoryBlkno left_blkno,
					  BTreePageItemLocator right_loc, OInMemoryBlkno right_blkno)
{
	Page		parent = O_GET_IN_MEMORY_PAGE(parent_blkno),
				left = O_GET_IN_MEMORY_PAGE(left_blkno),
				right = O_GET_IN_MEMORY_PAGE(right_blkno);
	OrioleDBPageDesc *right_desc;
	BTreePageHeader *left_header = (BTreePageHeader *) left;
	FileExtent	right_extent;
	CommitSeqNo csn;
	UndoLocation undo_loc;
	uint32		checkpoint_number;
	bool		copy_blkno;
	bool		needsUndo;

	if (RightLinkIsValid(BTREE_PAGE_GET_RIGHTLINK(right)))
	{
		/* concurrent split in progress */
		return false;
	}

	if (!get_checkpoint_number(desc, right_blkno,
							   &checkpoint_number, &copy_blkno))
	{
		/*
		 * page is concurrent to in progress checkpoint and can not be merged
		 */
		return false;
	}

	needsUndo = O_PAGE_IS(left, LEAF) && desc->undoType != UndoReserveNone;
	if (needsUndo && OXidIsValid(desc->createOxid) &&
		!XACT_INFO_IS_FINISHED(desc->createOxid))
		needsUndo = false;

	if (needsUndo)
		csn = pg_atomic_fetch_add_u64(&ShmemVariableCache->nextCommitSeqNo, 1);
	else
		csn = COMMITSEQNO_INPROGRESS;

	if (!can_be_merged(desc, left, right, csn))
	{
		return false;
	}

	/* all checks are done, errors do not expected after this line */
	START_CRIT_SECTION();

	/* deletes downlink to right page from the parent node */
	page_block_reads(parent_blkno);

	page_locator_delete_item(parent, &right_loc);
	MARK_DIRTY(desc->ppool, parent_blkno);

	/* unlocks the parent page */
	if (*merge_parent && is_page_too_sparse(desc, parent))
	{
		/*
		 * We can try to merge thr parent page in the loop.  No undo is
		 * required for non-leaf pages.
		 */
		if (!O_PAGE_IS(parent, RIGHTMOST))
			copy_fixed_hikey(desc, parent_hikey, parent);
		else
			O_TUPLE_SET_NULL(parent_hikey->tuple);
		unlock_page(parent_blkno);
	}
	else
	{
		/* no need to merge parent page */
		unlock_page(parent_blkno);
		parent_blkno = OInvalidInMemoryBlkno;
		*merge_parent = false;
	}

	/* Make a page-level undo item if needed */
	if (needsUndo)
	{
		undo_loc = make_merge_undo_image(desc, left, right, csn);
		Assert(UndoLocationIsValid(undo_loc));

		/*
		 * Memory barrier between making undo image and setting the undo
		 * location.
		 */
		pg_write_barrier();
	}
	else
	{
		undo_loc = InvalidUndoLocation;
	}

	/*
	 * Merge the pages and remove rightlink to the right page.
	 *
	 * It contains the required memory barrier between making undo image and
	 * setting the undo location.
	 */
	merge_pages(desc, left_blkno, right, csn);
	btree_page_update_max_key_len(desc, left);
	MARK_DIRTY(desc->ppool, left_blkno);

	/* the right page can not be found in B-Tree after this line */

	left_header->undoLocation = undo_loc;

	/*
	 * Memory barrier between write undo location and csn.  See comment in the
	 * o_btree_read_page() for details.
	 */
	pg_write_barrier();
	left_header->csn = csn;
	unlock_page(left_blkno);
	left_blkno = OInvalidInMemoryBlkno;

	right_desc = O_GET_IN_MEMORY_PAGEDESC(right_blkno);
	right_extent = right_desc->fileExtent;

	CLEAN_DIRTY(desc->ppool, right_blkno);
	O_PAGE_CHANGE_COUNT_INC(right);

	ppool_free_page(desc->ppool, right_blkno, true);

	if (O_PAGE_IS(left, LEAF))
		pg_atomic_fetch_sub_u32(&BTREE_GET_META(desc)->leafPagesNum, 1);

	END_CRIT_SECTION();

	if (FileExtentIsValid(right_extent))
	{
		free_extent_for_checkpoint(desc, &right_extent,
								   checkpoint_number);
	}

	return true;
}


/*
 * Returns true if page is successfully merged to the left or to the right.
 */
bool
btree_try_merge_and_unlock(BTreeDescr *desc, OInMemoryBlkno blkno,
						   bool nested, bool wait_io)
{
	BTreePageItemLocator target_loc,
				left_loc,
				right_loc;
	Page		target = O_GET_IN_MEMORY_PAGE(blkno),
				parent,
				right,
				left;
	OFixedKey	key;
	int			level;
	OBTreeFindPageContext find_context;
	OInMemoryBlkno parent_blkno,
				target_blkno = OInvalidInMemoryBlkno,
				right_blkno,
				left_blkno;
	uint32		parent_change_count;
	bool		success = false;
	bool		needsUndo = desc->undoType != UndoReserveNone;

	/*
	 * Reserve the required undo size.  We are holding the page lock, so we
	 * can only do this with 'wait == false'.
	 */
	if (needsUndo && !reserve_undo_size_extended(desc->undoType,
												 2 * O_MERGE_UNDO_IMAGE_SIZE,
												 false, false))
	{
		/* unable to reserve undo location, no opportunity to resume */
		unlock_page(blkno);
		Assert(!have_locked_pages());
		return false;
	}

	/* Step 1: get all the information from the parent page */
	level = PAGE_GET_LEVEL(target);

	Assert(page_is_locked(blkno));
	Assert(desc->rootInfo.rootPageBlkno != blkno);
	Assert(is_page_too_sparse(desc, target));
	Assert(!O_PAGE_IS(target, LEFTMOST) || !O_PAGE_IS(target, RIGHTMOST));
	Assert(!RightLinkIsValid(BTREE_PAGE_GET_RIGHTLINK(target)));

	page_block_reads(blkno);

	/* copy hikey of current page */
	if (!O_PAGE_IS(target, RIGHTMOST))
		copy_fixed_hikey(desc, &key, target);
	else
		O_TUPLE_SET_NULL(key.tuple);

	/* unlock current page */
	unlock_page(blkno);

	/*
	 * Step 2: refind the parent.  We did release the target lock first: locks
	 * shouldn't go bottom-up.
	 */
	init_page_find_context(&find_context, desc, COMMITSEQNO_INPROGRESS, BTREE_PAGE_FIND_MODIFY | BTREE_PAGE_FIND_DOWNLINK_LOCATION);

	/* get a full find context for parent page and lock it */
	if (!O_TUPLE_IS_NULL(key.tuple))
		find_page(&find_context, &key.tuple, BTreeKeyPageHiKey, level + 1);
	else
		find_page(&find_context, NULL, BTreeKeyRightmost, level + 1);
	parent_blkno = find_context.items[find_context.index].blkno;
	parent_change_count = find_context.items[find_context.index].pageChangeCount;

	while (true)
	{
		BTreeNonLeafTuphdr *target_tuph,
				   *right_tuph,
				   *left_tuph;
		bool		merge_parent,
					merged = false;

		if (!page_is_locked(parent_blkno))
		{
			/* refind parent page if needed */
			if (!O_TUPLE_IS_NULL(key.tuple))
				refind_page(&find_context, &key.tuple, BTreeKeyPageHiKey, level + 1,
							parent_blkno,
							parent_change_count);
			else
				refind_page(&find_context, NULL, BTreeKeyRightmost, level + 1,
							parent_blkno,
							parent_change_count);
		}

		/* Step 3: do all the checks with parent and target */
		parent_change_count = find_context.items[find_context.index].pageChangeCount;
		parent_blkno = find_context.items[find_context.index].blkno;
		Assert(page_is_locked(parent_blkno));
		parent = O_GET_IN_MEMORY_PAGE(parent_blkno);

		target_loc = find_context.items[find_context.index].locator;
		target_tuph = (BTreeNonLeafTuphdr *) BTREE_PAGE_LOCATOR_GET_ITEM(parent, &target_loc);

		if (!DOWNLINK_IS_IN_MEMORY(target_tuph->downlink))
		{
			/*
			 * Page with O_BTREE_FLAG_UNDER_MERGE can not be evicted. But it
			 * can be split or merged and evicted.
			 */
			unlock_page(parent_blkno);
			break;
		}

		target_blkno = DOWNLINK_GET_IN_MEMORY_BLKNO(target_tuph->downlink);

		/* all ok, lock target page */
		lock_page(target_blkno);
		target = O_GET_IN_MEMORY_PAGE(target_blkno);
		Assert((level == 0) == O_PAGE_IS(target, LEAF));

		if (BTREE_PAGE_ITEMS_COUNT(parent) == 1 ||
			RightLinkIsValid(BTREE_PAGE_GET_RIGHTLINK(target)))
		{
			/*
			 * The target page is a single child of parent node or concurrent
			 * split in progress.
			 */
			unlock_page(parent_blkno);
			unlock_page(target_blkno);
			break;
		}

		if (page_is_under_checkpoint(desc, parent_blkno)
			|| (level > 0 && page_is_under_checkpoint(desc, target_blkno)))
		{
			/* pages merge is concurrent to in progress checkpoint */
			unlock_page(parent_blkno);
			unlock_page(target_blkno);
			break;
		}

		merge_parent = (nested && find_context.index > 0);

		/*
		 * Step 4: try to merge to the right.  On success, all page lock are
		 * released.  On failure, target and parent page locks are held.
		 */
		if (BTREE_PAGE_LOCATOR_GET_OFFSET(parent, &target_loc) + 1 <
			BTREE_PAGE_ITEMS_COUNT(parent))
		{
			right_loc = target_loc;
			BTREE_PAGE_LOCATOR_NEXT(parent, &right_loc);
			right_tuph = (BTreeNonLeafTuphdr *) BTREE_PAGE_LOCATOR_GET_ITEM(parent, &right_loc);
			if (DOWNLINK_IS_IN_IO(right_tuph->downlink) && wait_io)
			{
				/* Wait till IO completion and retry */
				uint32		io_num = DOWNLINK_GET_IO_LOCKNUM(right_tuph->downlink);

				unlock_page(parent_blkno);
				unlock_page(target_blkno);
				target_blkno = OInvalidInMemoryBlkno;
				wait_for_io_completion(io_num);
				continue;
			}
			else if (DOWNLINK_IS_IN_MEMORY(right_tuph->downlink))
			{
				int			io_num;

				Assert(DOWNLINK_IS_IN_MEMORY(right_tuph->downlink));
				right_blkno = DOWNLINK_GET_IN_MEMORY_BLKNO(right_tuph->downlink);
				lock_page(right_blkno);
				right = O_GET_IN_MEMORY_PAGE(right_blkno);

				io_num = O_GET_IN_MEMORY_PAGEDESC(right_blkno)->ionum;
				if (io_num >= 0 && wait_io)
				{
					unlock_page(parent_blkno);
					unlock_page(target_blkno);
					unlock_page(right_blkno);
					target_blkno = OInvalidInMemoryBlkno;
					wait_for_io_completion(io_num);
					continue;
				}

				if (!O_PAGE_IS(right, PRE_CLEANUP) &&
					!RightLinkIsValid(BTREE_PAGE_GET_RIGHTLINK(right)) &&
					!page_is_under_checkpoint(desc, right_blkno) &&
					io_num < 0)
				{
					merged = btree_try_merge_pages(desc, parent_blkno, &key,
												   &merge_parent, target_blkno,
												   right_loc, right_blkno);
					if (!merged)
						unlock_page(right_blkno);
				}
				else
				{
					merged = false;
					unlock_page(right_blkno);
				}
			}
		}

		/*
		 * Step 5: try to merge to the left.  On success, all page lock are
		 * released.  On failure, target and parent page locks are held.
		 */
		if (!merged && BTREE_PAGE_LOCATOR_GET_OFFSET(parent, &target_loc) > 0)
		{
			left_loc = target_loc;
			BTREE_PAGE_LOCATOR_PREV(parent, &left_loc);
			left_tuph = (BTreeNonLeafTuphdr *) BTREE_PAGE_LOCATOR_GET_ITEM(parent, &left_loc);
			if (DOWNLINK_IS_IN_IO(left_tuph->downlink) && wait_io)
			{
				/* Wait till IO completion and retry */
				uint32		io_num = DOWNLINK_GET_IO_LOCKNUM(left_tuph->downlink);

				unlock_page(parent_blkno);
				unlock_page(target_blkno);
				target_blkno = OInvalidInMemoryBlkno;
				wait_for_io_completion(io_num);
				continue;
			}
			else if (DOWNLINK_IS_IN_MEMORY(left_tuph->downlink))
			{
				int			io_num;

				Assert(DOWNLINK_IS_IN_MEMORY(left_tuph->downlink));
				left_blkno = DOWNLINK_GET_IN_MEMORY_BLKNO(left_tuph->downlink);
				lock_page(left_blkno);
				left = O_GET_IN_MEMORY_PAGE(left_blkno);

				io_num = O_GET_IN_MEMORY_PAGEDESC(target_blkno)->ionum;
				if (io_num >= 0 && wait_io)
				{
					unlock_page(parent_blkno);
					unlock_page(left_blkno);
					unlock_page(target_blkno);
					target_blkno = OInvalidInMemoryBlkno;
					wait_for_io_completion(io_num);
					continue;
				}

				if (!O_PAGE_IS(left, PRE_CLEANUP) &&
					!RightLinkIsValid(BTREE_PAGE_GET_RIGHTLINK(left)) &&
					!page_is_under_checkpoint(desc, left_blkno) &&
					io_num < 0)
				{
					merged = btree_try_merge_pages(desc, parent_blkno,
												   &key, &merge_parent,
												   left_blkno,
												   target_loc, target_blkno);
					if (!merged)
						unlock_page(left_blkno);
				}
				else
				{
					merged = false;
					unlock_page(left_blkno);
				}
			}
		}

		if (!merged)
		{
			unlock_page(parent_blkno);
			unlock_page(target_blkno);
			break;
		}
		else
		{
			success = true;
		}

		if (merge_parent)
		{
			blkno = parent_blkno;
			find_context.index--;
			parent_blkno = find_context.items[find_context.index].blkno;
			parent_change_count = find_context.items[find_context.index].pageChangeCount;
			level++;
		}
		else
		{
			break;
		}
		/* else we will try to merge the parent page in the loop */
	}

	if (needsUndo)
		release_undo_size(desc->undoType);

	Assert(!have_locked_pages());
	return success;
}

/*
 * Checks is pages can be merged.
 */
static bool
can_be_merged(BTreeDescr *desc, Page left, Page right, CommitSeqNo csn)
{
	LocationIndex space_free,
				space_needed;
	bool		is_leaf = O_PAGE_IS(left, LEAF);

	Assert(O_PAGE_IS(left, LEAF) == O_PAGE_IS(right, LEAF));
	Assert(!O_PAGE_IS(left, RIGHTMOST));

	space_free = BTREE_PAGE_FREE_SPACE(left);
	space_needed = ORIOLEDB_BLCKSZ - BTREE_PAGE_FREE_SPACE(right);

	/* we can not compact a node */
	if (!is_leaf)
		return space_free >= space_needed;

	/* no need to compact page */
	if (space_free >= space_needed)
		return true;

	/* we can merge pages after the pages compaction */
	if (space_free + PAGE_GET_N_VACATED(left) +
		PAGE_GET_N_VACATED(right) < space_needed)
		return false;

	if (space_free + page_get_vacated_space(desc, left, csn) +
		page_get_vacated_space(desc, right, csn) >= space_needed)
		return true;

	/* we can not merge this pages */
	return false;
}

/*
 * Merges pages and writes result to the left page.
 */
static void
merge_pages(BTreeDescr *desc, OInMemoryBlkno left_blkno,
			Page right, CommitSeqNo csn)
{
	Page		left = O_GET_IN_MEMORY_PAGE(left_blkno);
	BTreePageHeader *left_header = (BTreePageHeader *) left,
			   *right_header = (BTreePageHeader *) right;
	OTuple		leftHikey,
				rightHikey;
	LocationIndex leftHikeySize,
				rightHikeySize;
	BTreePageItemLocator loc;
	BTreePageItem items[BTREE_PAGE_MAX_CHUNK_ITEMS];
	int			i;
	bool		leaf = O_PAGE_IS(left, LEAF);
	bool		first;
	char		newItem[Max(BTreeLeafTuphdrSize, BTreeNonLeafTuphdrSize) + O_BTREE_MAX_TUPLE_SIZE];

	Assert(O_PAGE_IS(left, LEAF) == O_PAGE_IS(right, LEAF));
	Assert(!O_PAGE_IS(left, RIGHTMOST));

	leftHikeySize = BTREE_PAGE_GET_HIKEY_SIZE(left);
	BTREE_PAGE_GET_HIKEY(leftHikey, left);
	if (O_PAGE_IS(right, RIGHTMOST))
	{
		rightHikeySize = 0;
		O_TUPLE_SET_NULL(rightHikey);
	}
	else
	{
		rightHikeySize = BTREE_PAGE_GET_HIKEY_SIZE(right);
		BTREE_PAGE_GET_HIKEY(rightHikey, right);
	}

	i = 0;
	if (leaf)
	{
		BTREE_PAGE_FOREACH_ITEMS(left, &loc)
		{
			BTreeLeafTuphdr *tupHdr;
			OTuple		tup;
			bool		finished;

			BTREE_PAGE_READ_LEAF_ITEM(tupHdr, tup, left, &loc);
			finished = XACT_INFO_FINISHED_FOR_EVERYBODY(tupHdr->xactInfo);
			if (finished && tupHdr->deleted)
			{
				if (COMMITSEQNO_IS_INPROGRESS(csn) || XACT_INFO_MAP_CSN(tupHdr->xactInfo) < csn)
					continue;
			}

			items[i].data = (Pointer) tupHdr;
			items[i].flags = tup.formatFlags;
			items[i].size = finished ? (BTreeLeafTuphdrSize + MAXALIGN(o_btree_len(desc, tup, OTupleLength))) :
				BTREE_PAGE_GET_ITEM_SIZE(left, &loc);
			items[i].newItem = false;
			i++;
		}
	}
	else
	{
		BTREE_PAGE_FOREACH_ITEMS(left, &loc)
		{
			items[i].data = BTREE_PAGE_LOCATOR_GET_ITEM(left, &loc);
			items[i].flags = BTREE_PAGE_GET_ITEM_FLAGS(left, &loc);
			items[i].size = BTREE_PAGE_GET_ITEM_SIZE(left, &loc);
			items[i].newItem = false;
			i++;
		}
	}

	if (leaf)
	{
		BTREE_PAGE_FOREACH_ITEMS(right, &loc)
		{
			BTreeLeafTuphdr *tupHdr;
			OTuple		tup;
			bool		finished;

			BTREE_PAGE_READ_LEAF_ITEM(tupHdr, tup, right, &loc);
			finished = XACT_INFO_FINISHED_FOR_EVERYBODY(tupHdr->xactInfo);
			if (finished && tupHdr->deleted)
			{
				if (COMMITSEQNO_IS_INPROGRESS(csn) || XACT_INFO_MAP_CSN(tupHdr->xactInfo) < csn)
					continue;
			}

			items[i].data = (Pointer) tupHdr;
			items[i].flags = tup.formatFlags;
			items[i].size = finished ? (BTreeLeafTuphdrSize + MAXALIGN(o_btree_len(desc, tup, OTupleLength))) :
				BTREE_PAGE_GET_ITEM_SIZE(right, &loc);
			items[i].newItem = false;
			i++;
		}
	}
	else
	{
		first = true;
		BTREE_PAGE_FOREACH_ITEMS(right, &loc)
		{
			if (first)
			{
				first = false;
				memcpy(newItem,
					   BTREE_PAGE_LOCATOR_GET_ITEM(right, &loc),
					   BTreeNonLeafTuphdrSize);
				memcpy(&newItem[BTreeNonLeafTuphdrSize],
					   leftHikey.data,
					   leftHikeySize);
				items[i].data = newItem;
				items[i].flags = leftHikey.formatFlags;
				items[i].size = MAXALIGN(BTreeNonLeafTuphdrSize + leftHikeySize);
				items[i].newItem = false;
				i++;
				continue;
			}
			items[i].data = BTREE_PAGE_LOCATOR_GET_ITEM(right, &loc);
			items[i].flags = BTREE_PAGE_GET_ITEM_FLAGS(right, &loc);
			items[i].size = BTREE_PAGE_GET_ITEM_SIZE(right, &loc);
			items[i].newItem = false;
			i++;
		}
	}

	page_block_reads(left_blkno);

	left_header->flags = left_header->flags | right_header->flags;

	btree_page_reorg(desc, left, items, i, rightHikeySize, rightHikey, NULL);

	o_btree_page_calculate_statistics(desc, left);

	left_header->rightLink = InvalidRightLink;
	left_header->prevInsertOffset = InvalidOffsetNumber;
}

/*
 * Returns true if page is too sparse and we can try to merge it.
 */
bool
is_page_too_sparse(BTreeDescr *desc, Page p)
{
	LocationIndex space_free;

	/* we can not merge rootPageBlkno page */
	if (O_PAGE_IS(p, RIGHTMOST) && O_PAGE_IS(p, LEFTMOST))
		return false;

	/* page should not be under split */
	if (RightLinkIsValid(BTREE_PAGE_GET_RIGHTLINK(p)))
		return false;

	if (O_PAGE_IS(p, LEAF))
	{
		/* if leaf have no items */
		if (BTREE_PAGE_ITEMS_COUNT(p) == 0)
			return true;

		space_free = BTREE_PAGE_FREE_SPACE(p) + PAGE_GET_N_VACATED(p);
		if (((double) space_free / ORIOLEDB_BLCKSZ) < O_MERGE_LEAF_FREE_RATIO)
			return false;

		space_free = BTREE_PAGE_FREE_SPACE(p) + page_get_vacated_space(desc, p, 0);
		return ((double) space_free / ORIOLEDB_BLCKSZ) >= O_MERGE_LEAF_FREE_RATIO;
	}
	else
	{
		/* if node have only one downlink */
		if (BTREE_PAGE_ITEMS_COUNT(p) == 1)
			return true;

		space_free = BTREE_PAGE_FREE_SPACE(p);
		return ((double) space_free / ORIOLEDB_BLCKSZ) >= O_MERGE_NODE_FREE_RATIO;
	}
}
