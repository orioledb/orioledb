/*-------------------------------------------------------------------------
 *
 * split.c
 *		Routines for implementation of splitting B-tree page.
 *
 * Copyright (c) 2021-2022, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/btree/split.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "orioledb.h"

#include "btree/find.h"
#include "btree/split.h"
#include "btree/page_chunks.h"
#include "btree/undo.h"
#include "checkpoint/checkpoint.h"
#include "recovery/recovery.h"
#include "transam/undo.h"
#include "utils/page_pool.h"
#include "utils/stopevent.h"

#include "miscadmin.h"
#include "utils/memutils.h"

typedef struct
{
	BTreeDescr *desc;
	Page		page;
	BTreePageItemLocator loc;
	OTuple		newitem;
	OffsetNumber newoffset;
	LocationIndex newitemSize;
	bool		replace;
	bool		newitemIsCur;
	CommitSeqNo csn;
} SplitItemIterator;

static void
init_split_item_interator(BTreeDescr *desc, SplitItemIterator *it, Page page,
						  OTuple newitem, LocationIndex newitemSize,
						  OffsetNumber newoffset, bool replace, bool last,
						  CommitSeqNo csn)
{
	it->desc = desc;
	it->page = page;
	it->newitem = newitem;
	it->newoffset = newoffset;
	it->newitemSize = newitemSize;
	it->replace = replace;
	if (!last)
	{
		BTREE_PAGE_LOCATOR_FIRST(it->page, &it->loc);
		it->newitemIsCur = (it->newoffset == 0);
	}
	else
	{
		OffsetNumber count = BTREE_PAGE_ITEMS_COUNT(page);

		BTREE_PAGE_LOCATOR_LAST(it->page, &it->loc);

		if (it->newoffset == count || (replace && it->newoffset == count - 1))
			it->newitemIsCur = true;
		else
			it->newitemIsCur = false;
	}
	it->csn = csn;
}

static void
split_item_interator_next(SplitItemIterator *it)
{
	if (it->newitemIsCur)
	{
		it->newitemIsCur = false;
		if (it->replace)
			BTREE_PAGE_LOCATOR_NEXT(it->page, &it->loc);
	}
	else
	{
		BTREE_PAGE_LOCATOR_NEXT(it->page, &it->loc);
		if (BTREE_PAGE_LOCATOR_GET_OFFSET(it->page, &it->loc) == it->newoffset)
			it->newitemIsCur = true;
	}
}

static void
split_item_interator_prev(SplitItemIterator *it)
{
	if (it->newitemIsCur)
	{
		it->newitemIsCur = false;
		BTREE_PAGE_LOCATOR_PREV(it->page, &it->loc);
	}
	else
	{
		if (!it->replace && BTREE_PAGE_LOCATOR_GET_OFFSET(it->page, &it->loc) == it->newoffset)
		{
			it->newitemIsCur = true;
			return;
		}

		BTREE_PAGE_LOCATOR_PREV(it->page, &it->loc);

		if (it->replace && BTREE_PAGE_LOCATOR_GET_OFFSET(it->page, &it->loc) == it->newoffset)
			it->newitemIsCur = true;
	}
}

static int
split_item_interator_size(SplitItemIterator *it)
{
	if (it->newitemIsCur)
	{
		return it->newitemSize + sizeof(LocationIndex);
	}
	else
	{
		/*
		 * Take into account that split will remove the tuples deleted by
		 * finished transactions, and resize tuples to minimal size.
		 */
		if (O_PAGE_IS(it->page, LEAF))
		{
			BTreeLeafTuphdr *tupHdr;
			OTuple		tup;
			bool		finished;

			BTREE_PAGE_READ_LEAF_ITEM(tupHdr, tup, it->page, &it->loc);
			finished = XACT_INFO_FINISHED_FOR_EVERYBODY(tupHdr->xactInfo);
			if (finished && tupHdr->deleted &&
				(COMMITSEQNO_IS_INPROGRESS(it->csn) || XACT_INFO_MAP_CSN(tupHdr->xactInfo) < it->csn))
			{
				return 0;
			}

			if (finished)
				return BTreeLeafTuphdrSize +
					MAXALIGN(o_btree_len(it->desc, tup, OTupleLength)) +
					sizeof(LocationIndex);
			else
				return BTREE_PAGE_GET_ITEM_SIZE(it->page, &it->loc) +
					sizeof(LocationIndex);
		}
		else
		{
			return BTREE_PAGE_GET_ITEM_SIZE(it->page, &it->loc) + sizeof(LocationIndex);
		}
	}
}

/*
 * Get current item from split iterator.
 */
static OTuple
split_item_interator_get(SplitItemIterator *it)
{
	OTuple		result;

	if (it->newitemIsCur)
		result = it->newitem;
	else
		BTREE_PAGE_READ_TUPLE(result, it->page, &it->loc);

	return result;
}

/*
 * Find the location for B-tree page split.  This function take into accouint
 * insertion of new tuple or replacement of existing one.  It tries to keep
 * as close as possible to `targetLocation`, or if `targetLocation == 0` close
 * to `spaceRatio`.  Also, this function takes advantage of reclaiming unused
 * space according to `csn`.  Returns number of items in new left page and
 * sets the first tuple of right page to `*split_item`.
 */
OffsetNumber
btree_page_split_location(BTreeDescr *desc, Page page, OffsetNumber offset,
						  LocationIndex tuplesize, OTuple tuple, bool replace,
						  OffsetNumber targetLocation, float4 spaceRatio,
						  OTuple *split_item, CommitSeqNo csn)
{
	int			leftPageSpaceLeft,
				rightPageSpaceLeft,
				minLeftPageItemsCount,
				maxLeftPageItemsCount,
				totalCount,
				maxKeyLen;
	LocationIndex newitem_size,
				hikeys_end;
	SplitItemIterator left_it,
				right_it;

	Assert(spaceRatio > 0.0f && spaceRatio < 1.0f);

	if (O_PAGE_IS(page, LEAF))
		newitem_size = BTreeLeafTuphdrSize + MAXALIGN(tuplesize);
	else
		newitem_size = BTreeNonLeafTuphdrSize + MAXALIGN(tuplesize);

	maxKeyLen = MAXALIGN(((BTreePageHeader *) page)->maxKeyLen);

	totalCount = BTREE_PAGE_ITEMS_COUNT(page) + (replace ? 0 : 1);
	hikeys_end = BTREE_PAGE_HIKEYS_END(desc, page);
	leftPageSpaceLeft = ORIOLEDB_BLCKSZ - Max(hikeys_end, MAXALIGN(sizeof(BTreePageHeader)) + maxKeyLen);
	if (!O_PAGE_IS(page, RIGHTMOST))
		rightPageSpaceLeft = ORIOLEDB_BLCKSZ - Max(hikeys_end, MAXALIGN(sizeof(BTreePageHeader)) + BTREE_PAGE_GET_HIKEY_SIZE(page));
	else
		rightPageSpaceLeft = ORIOLEDB_BLCKSZ - hikeys_end;

	/*
	 * Left page should contain at least one item, and leaves at lest one item
	 * for the right page.
	 */
	minLeftPageItemsCount = 1;
	maxLeftPageItemsCount = totalCount - 1;
	init_split_item_interator(desc, &left_it, page, tuple, newitem_size, offset,
							  replace, false, csn);
	init_split_item_interator(desc, &right_it, page, tuple, newitem_size, offset,
							  replace, true, csn);
	leftPageSpaceLeft -= split_item_interator_size(&left_it);
	rightPageSpaceLeft -= split_item_interator_size(&right_it);
	split_item_interator_next(&left_it);
	split_item_interator_prev(&right_it);

	Assert(leftPageSpaceLeft >= 0 && rightPageSpaceLeft >= 0);

	/*
	 * Shift minimal and maximal left page item counts till they are equal.
	 */
	while (minLeftPageItemsCount != maxLeftPageItemsCount)
	{
		Assert(minLeftPageItemsCount < maxLeftPageItemsCount);

		/*
		 * Choose page to add item.  At first only we try place new item to
		 * the page that have a space yet.  Then, we try to follow
		 * `targetLocation`.  If `targetLocation` isn't given, then follow
		 * `spaceRatio`.
		 */
		if (rightPageSpaceLeft <= 0 || (leftPageSpaceLeft > 0 &&
										(targetLocation == 0 ?
										 (float4) leftPageSpaceLeft * spaceRatio > (float4) rightPageSpaceLeft * (1.0f - spaceRatio) :
										 minLeftPageItemsCount < targetLocation)))
		{
			/* Try place item to the left page */
			Assert(leftPageSpaceLeft > 0);
			leftPageSpaceLeft -= split_item_interator_size(&left_it);
			if (leftPageSpaceLeft < 0)
				continue;
			split_item_interator_next(&left_it);
			minLeftPageItemsCount++;
		}
		else
		{
			/* Try place item to the right page */
			Assert(rightPageSpaceLeft > 0);
			rightPageSpaceLeft -= split_item_interator_size(&right_it);
			if (rightPageSpaceLeft < 0)
			{
				continue;
			}
			split_item_interator_prev(&right_it);
			maxLeftPageItemsCount--;
		}
	}

	if (split_item)
		*split_item = split_item_interator_get(&left_it);

	return minLeftPageItemsCount;
}

OffsetNumber
btree_get_split_left_count(BTreeDescr *desc, OInMemoryBlkno blkno,
						   OTuple tuple, LocationIndex tuplesize,
						   OffsetNumber offset, bool replace,
						   OTuple *split_key, LocationIndex *split_key_len,
						   CommitSeqNo csn)
{
	Page		page = O_GET_IN_MEMORY_PAGE(blkno);
	BTreePageHeader *header = (BTreePageHeader *) page;
	OffsetNumber targetCount;
	OffsetNumber result;
	float4		spaceRatio;
	OTuple		split_item;

	/* The default target is to split the page 50%/50% */
	targetCount = 0;
	spaceRatio = 0.5;

	/*
	 * Try to autodetect ordered inserts and split near the insertion point.
	 * If we're close to the end of the page, split already inserted data away
	 * from the insertion point (if it gives at least 90% utilization).
	 * Otherwise, place already inserted data together with the insertion
	 * point. Hopefuly, we still have many tuple to insert and that will give
	 * us the good utilization.
	 */
	if (offset == header->prevInsertOffset + 1)
	{
		if ((float) offset / (float) header->itemsCount >= 0.9)
			targetCount = offset;
		else
			targetCount = offset + 1;
	}
	else if ((!replace && offset == header->prevInsertOffset) ||
			 (replace && offset == header->prevInsertOffset - 1))
	{
		if ((float) offset / (float) header->itemsCount <= 0.1)
			targetCount = offset + 1;
		else
			targetCount = offset;
	}

	/*
	 * If we don't autodetect the insertion order, we still assume TOAST and
	 * rightmost inserts are always assumed to be ordered ascendingly.
	 */
	else if ((desc->type == oIndexToast && O_PAGE_IS(page, LEAF)) || O_PAGE_IS(page, RIGHTMOST))
		spaceRatio = 0.9;

	result = btree_page_split_location(desc, page, offset, tuplesize, tuple, replace,
									   targetCount, spaceRatio, &split_item, csn);

	/*
	 * Fill the split key.  Convert tuple to key if needed.
	 */
	if (split_key)
	{
		bool		allocated = true;

		if (O_PAGE_IS(page, LEAF))
			split_item = o_btree_tuple_make_key(desc, split_item, NULL,
												false, &allocated);

		*split_key_len = o_btree_len(desc, split_item, OKeyLength);
		if (!O_PAGE_IS(page, LEAF) || !allocated)
		{
			split_key->data = (Pointer) palloc(*split_key_len);
			split_key->formatFlags = split_item.formatFlags;
			memcpy(split_key->data, split_item.data, *split_key_len);
		}
		else
		{
			*split_key = split_item;
		}
	}

	return result;
}

/*
 * Split B-tree page into two.
 *
 * Returns OInvalidInMemoryBlkno if the page can not be split due to the fact that
 * it is under processing by the checkpointer worker.
 */
void
perform_page_split(BTreeDescr *desc, OInMemoryBlkno blkno, OInMemoryBlkno new_blkno,
				   OffsetNumber left_count, OTuple splitkey,
				   LocationIndex splitkey_len, OffsetNumber *offset, bool *place_right,
				   Pointer tupleheader, OTuple tuple, LocationIndex tuplesize,
				   bool replace, CommitSeqNo csn, UndoLocation undoLoc)
{
	Page		left_page = O_GET_IN_MEMORY_PAGE(blkno),
				right_page = O_GET_IN_MEMORY_PAGE(new_blkno);
	BTreePageHeader *left_header = (BTreePageHeader *) left_page,
			   *right_header = (BTreePageHeader *) right_page;
	bool		leaf = O_PAGE_IS(left_page, LEAF);
	OTuple		hikey;
	LocationIndex hikeySize;
	int			i,
				count;
	LocationIndex tuple_header_size = leaf ? BTreeLeafTuphdrSize : BTreeNonLeafTuphdrSize;
	BTreePageItemLocator loc;
	BTreePageItem items[BTREE_PAGE_MAX_CHUNK_ITEMS + 1];
	char		newItem[Max(BTreeLeafTuphdrSize, BTreeNonLeafTuphdrSize) + O_BTREE_MAX_TUPLE_SIZE];

	init_new_btree_page(desc, new_blkno,
						left_header->flags & ~(O_BTREE_FLAG_LEFTMOST),
						PAGE_GET_LEVEL(left_page), false);

	/* Fill the array of items for btree_page_reorg() function */
	i = 0;
	BTREE_PAGE_LOCATOR_FIRST(left_page, &loc);
	while (BTREE_PAGE_LOCATOR_IS_VALID(left_page, &loc) || i == *offset)
	{
		if (i == *offset)
		{
			memcpy(newItem, tupleheader, tuple_header_size);
			memcpy(&newItem[tuple_header_size], tuple.data, tuplesize);
			if (tuplesize != MAXALIGN(tuplesize))
				memset(&newItem[tuple_header_size + tuplesize], 0, MAXALIGN(tuplesize) - tuplesize);
			items[i].data = newItem;
			items[i].flags = tuple.formatFlags;
			items[i].size = tuple_header_size + MAXALIGN(tuplesize);
			items[i].newItem = false;
#ifdef ORIOLEDB_CUT_FIRST_KEY
			if (!leaf && i == left_count)
				items[i].size = BTreeNonLeafTuphdrSize;
#endif
			i++;
			if (replace)
			{
				BTREE_PAGE_LOCATOR_NEXT(left_page, &loc);
				continue;
			}
		}

		if (!BTREE_PAGE_LOCATOR_IS_VALID(left_page, &loc))
			break;

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

			BTREE_PAGE_READ_LEAF_ITEM(tupHdr, tup, left_page, &loc);
			finished = XACT_INFO_FINISHED_FOR_EVERYBODY(tupHdr->xactInfo);
			if (finished && tupHdr->deleted && i != left_count &&
				(COMMITSEQNO_IS_INPROGRESS(csn) || XACT_INFO_MAP_CSN(tupHdr->xactInfo) < csn))
			{
				if (i < left_count)
					left_count--;
				if (i < *offset)
					(*offset)--;
				BTREE_PAGE_LOCATOR_NEXT(left_page, &loc);
				continue;
			}

			items[i].data = (Pointer) tupHdr;
			items[i].flags = tup.formatFlags;
			items[i].size = finished ?
				(BTreeLeafTuphdrSize + MAXALIGN(o_btree_len(desc, tup, OTupleLength))) :
				BTREE_PAGE_GET_ITEM_SIZE(left_page, &loc);
			items[i].newItem = false;
		}
		else
		{
			items[i].data = BTREE_PAGE_LOCATOR_GET_ITEM(left_page, &loc);
			items[i].flags = BTREE_PAGE_GET_ITEM_FLAGS(left_page, &loc);
			items[i].size = BTREE_PAGE_GET_ITEM_SIZE(left_page, &loc);
			items[i].newItem = false;
		}

#ifdef ORIOLEDB_CUT_FIRST_KEY
		if (!leaf && i == left_count)
			items[i].size = BTreeNonLeafTuphdrSize;
#endif
		i++;
		BTREE_PAGE_LOCATOR_NEXT(left_page, &loc);
	}
	count = i;

	if (*offset < left_count)
	{
		*place_right = false;
	}
	else
	{
		*offset -= left_count;
		*place_right = true;
	}

	if (O_PAGE_IS(left_page, RIGHTMOST))
	{
		hikeySize = 0;
		O_TUPLE_SET_NULL(hikey);
	}
	else
	{
		hikeySize = BTREE_PAGE_GET_HIKEY_SIZE(left_page);
		BTREE_PAGE_GET_HIKEY(hikey, left_page);
	}

	btree_page_reorg(desc, right_page, &items[left_count], count - left_count,
					 hikeySize, hikey, NULL);

	/*
	 * Start page modification.  It contains the required memory barrier
	 * between making undo image and setting the undo location.
	 */
	page_block_reads(blkno);

	/* Link undo record with pages */
	left_header->undoLocation = undoLoc;
	right_header->undoLocation = undoLoc;

	/*
	 * Memory barrier between write undo location and csn.  See comment in the
	 * o_btree_read_page() for details.
	 */
	pg_write_barrier();

	left_header->csn = csn;
	right_header->csn = csn;
	left_header->rightLink = MAKE_IN_MEMORY_RIGHTLINK(new_blkno,
													  O_PAGE_GET_CHANGE_COUNT(right_page));
	left_header->flags &= ~(O_BTREE_FLAG_RIGHTMOST);

	btree_page_reorg(desc, left_page, &items[0], left_count,
					 splitkey_len, splitkey, NULL);

	o_btree_page_calculate_statistics(desc, left_page);
	o_btree_page_calculate_statistics(desc, right_page);

	MARK_DIRTY(desc->ppool, blkno);
	MARK_DIRTY(desc->ppool, new_blkno);
}
