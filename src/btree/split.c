/*-------------------------------------------------------------------------
 *
 * split.c
 *		Routines for implementation of splitting B-tree page.
 *
 * Copyright (c) 2021-2025, Oriole DB Inc.
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

void
make_split_items(BTreeDescr *desc, Page page,
				 BTreeSplitItems *items,
				 OffsetNumber *offset, Pointer tupleheader, OTuple tuple,
				 LocationIndex tuplesize, bool replace, CommitSeqNo csn)
{
	BTreePageItemLocator loc;
	bool		leaf = O_PAGE_IS(page, LEAF);
	LocationIndex tuple_header_size = leaf ? BTreeLeafTuphdrSize : BTreeNonLeafTuphdrSize;
	int			i;
	static char newItem[Max(BTreeLeafTuphdrSize, BTreeNonLeafTuphdrSize) + O_BTREE_MAX_TUPLE_SIZE];
	int			maxKeyLen = MAXALIGN(((BTreePageHeader *) page)->maxKeyLen);

	i = 0;
	BTREE_PAGE_LOCATOR_FIRST(page, &loc);
	while (BTREE_PAGE_LOCATOR_IS_VALID(page, &loc) || i == *offset)
	{
		if (i == *offset)
		{
			int			newKeyLen;

			memcpy(newItem, tupleheader, tuple_header_size);
			memcpy(&newItem[tuple_header_size], tuple.data, tuplesize);
			if (tuplesize != MAXALIGN(tuplesize))
				memset(&newItem[tuple_header_size + tuplesize], 0, MAXALIGN(tuplesize) - tuplesize);
			items->items[i].data = newItem;
			items->items[i].flags = tuple.formatFlags;
			items->items[i].size = tuple_header_size + MAXALIGN(tuplesize);
			items->items[i].newItem = false;
			newKeyLen = o_btree_len(desc, tuple, leaf ? OTupleKeyLengthNoVersion : OKeyLength);
			maxKeyLen = Max(maxKeyLen, newKeyLen);
			i++;
			if (replace)
			{
				BTREE_PAGE_LOCATOR_NEXT(page, &loc);
				continue;
			}
		}

		if (!BTREE_PAGE_LOCATOR_IS_VALID(page, &loc))
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

			BTREE_PAGE_READ_LEAF_ITEM(tupHdr, tup, page, &loc);
			finished = COMMITSEQNO_IS_FROZEN(csn) ? false : XACT_INFO_FINISHED_FOR_EVERYBODY(tupHdr->xactInfo);
			if (finished && tupHdr->deleted &&
				(COMMITSEQNO_IS_INPROGRESS(csn) || XACT_INFO_MAP_CSN(tupHdr->xactInfo) < csn))
			{
				if (i < *offset)
					(*offset)--;
				BTREE_PAGE_LOCATOR_NEXT(page, &loc);
				continue;
			}

			items->items[i].data = (Pointer) tupHdr;
			items->items[i].flags = tup.formatFlags;
			items->items[i].size = finished ?
				(BTreeLeafTuphdrSize + MAXALIGN(o_btree_len(desc, tup, OTupleLength))) :
				BTREE_PAGE_GET_ITEM_SIZE(page, &loc);
			items->items[i].newItem = false;
		}
		else
		{
			items->items[i].data = BTREE_PAGE_LOCATOR_GET_ITEM(page, &loc);
			items->items[i].flags = BTREE_PAGE_GET_ITEM_FLAGS(page, &loc);
			items->items[i].size = BTREE_PAGE_GET_ITEM_SIZE(page, &loc);
			items->items[i].newItem = false;
		}

		i++;
		BTREE_PAGE_LOCATOR_NEXT(page, &loc);
	}
	items->itemsCount = i;
	items->maxKeyLen = maxKeyLen;
	items->hikeySize = O_PAGE_IS(page, RIGHTMOST) ? 0 : BTREE_PAGE_GET_HIKEY_SIZE(page);
	items->hikeysEnd = BTREE_PAGE_HIKEYS_END(desc, page);
	items->leaf = O_PAGE_IS(page, LEAF);
}

void
perform_page_compaction(BTreeDescr *desc, OInMemoryBlkno blkno,
						BTreeSplitItems *items, bool needsUndo,
						CommitSeqNo csn)
{
	Page		p = O_GET_IN_MEMORY_PAGE(blkno);
	BTreePageHeader *header = (BTreePageHeader *) p;
	UndoLocation undoLocation;
	OFixedKey	hikey;
	LocationIndex hikeySize;

	START_CRIT_SECTION();

	Assert(O_PAGE_IS(p, LEAF));

	/* Make a page-level undo item if needed */
	if (needsUndo)
	{
		undoLocation = page_add_image_to_undo(desc, p, csn, NULL, 0);

		/*
		 * Start page modification.  It contains the required memory barrier
		 * between making undo image and setting the undo location.
		 */
		page_block_reads(blkno);

		/* Update the old page meta-data */

		header->undoLocation = undoLocation;
		header->prevInsertOffset = MaxOffsetNumber;

		/*
		 * Memory barrier between write undo location and csn.  See comment in
		 * the o_btree_read_page() for details.
		 */
		pg_write_barrier();

		header->csn = csn;
	}
	else
	{
		page_block_reads(blkno);
	}

	if (O_PAGE_IS(p, RIGHTMOST))
	{
		O_TUPLE_SET_NULL(hikey.tuple);
		hikeySize = 0;
	}
	else
	{
		copy_fixed_hikey(desc, &hikey, p);
		hikeySize = BTREE_PAGE_GET_HIKEY_SIZE(p);
	}

	btree_page_reorg(desc, p, items->items,
					 items->itemsCount, hikeySize, hikey.tuple, NULL);
	Assert(header->dataSize <= ORIOLEDB_BLCKSZ);
	o_btree_page_calculate_statistics(desc, p);

	END_CRIT_SECTION();
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
btree_page_split_location(BTreeDescr *desc,
						  BTreeSplitItems *items,
						  OffsetNumber targetLocation, float4 spaceRatio,
						  OTuple *split_item)
{
	int			leftPageSpaceLeft,
				rightPageSpaceLeft,
				minLeftPageItemsCount,
				maxLeftPageItemsCount;

	Assert(spaceRatio >= 0.0f && spaceRatio <= 1.0f);

	leftPageSpaceLeft = ORIOLEDB_BLCKSZ - Max(items->hikeysEnd, MAXALIGN(sizeof(BTreePageHeader)) + items->maxKeyLen);
	rightPageSpaceLeft = ORIOLEDB_BLCKSZ - Max(items->hikeysEnd, MAXALIGN(sizeof(BTreePageHeader)) + items->hikeySize);

	/*
	 * Left page should contain at least one item, and leaves at lest one item
	 * for the right page.
	 */
	minLeftPageItemsCount = 1;
	maxLeftPageItemsCount = items->itemsCount - 1;
	leftPageSpaceLeft -= items->items[0].size + MAXALIGN(sizeof(LocationIndex));
	rightPageSpaceLeft -= items->items[items->itemsCount - 1].size + MAXALIGN(sizeof(LocationIndex));

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
			leftPageSpaceLeft -= items->items[minLeftPageItemsCount].size +
				MAXALIGN(sizeof(LocationIndex) * (minLeftPageItemsCount + 1)) -
				MAXALIGN(sizeof(LocationIndex) * minLeftPageItemsCount);
			if (leftPageSpaceLeft < 0)
				continue;
			minLeftPageItemsCount++;
		}
		else
		{
			/* Try place item to the right page */
			Assert(rightPageSpaceLeft > 0);
			rightPageSpaceLeft -= items->items[maxLeftPageItemsCount - 1].size +
				MAXALIGN(sizeof(LocationIndex) * (items->itemsCount - maxLeftPageItemsCount + 1)) -
				MAXALIGN(sizeof(LocationIndex) * (items->itemsCount - maxLeftPageItemsCount));
			if (rightPageSpaceLeft < 0)
			{
				continue;
			}
			maxLeftPageItemsCount--;
		}
	}

	if (split_item)
	{
		split_item->formatFlags = items->items[minLeftPageItemsCount].flags;
		split_item->data = items->items[minLeftPageItemsCount].data +
			(items->leaf ? BTreeLeafTuphdrSize : BTreeNonLeafTuphdrSize);
	}

	return minLeftPageItemsCount;
}

OffsetNumber
btree_get_split_left_count(BTreeDescr *desc, Page page,
						   OffsetNumber offset, bool replace,
						   BTreeSplitItems *items,
						   OTuple *split_key, LocationIndex *split_key_len)
{
	BTreePageHeader *header = (BTreePageHeader *) page;
	OffsetNumber targetCount;
	OffsetNumber result;
	float4		spaceRatio;
	float4		fillfactorRatio = ((float4) desc->fillfactor) / 100.0f;
	OTuple		split_item;

	/* The default target is to split the page 50%/50% */
	targetCount = 0;
	spaceRatio = 0.5f;

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
		if ((float) offset / (float) header->itemsCount > fillfactorRatio)
			spaceRatio = fillfactorRatio;
		else if ((float) offset / (float) header->itemsCount >= 0.9f)
			targetCount = offset;
		else
			targetCount = offset + 1;
	}
	else if ((!replace && offset == header->prevInsertOffset) ||
			 (replace && offset == header->prevInsertOffset - 1))
	{
		if ((float) offset / (float) header->itemsCount < 1.0f - fillfactorRatio)
			spaceRatio = 1.0f - fillfactorRatio;
		else if ((float) offset / (float) header->itemsCount <= 0.1f)
			targetCount = offset + 1;
		else
			targetCount = offset;
	}

	/*
	 * If we don't autodetect the insertion order, we still assume TOAST and
	 * rightmost inserts are always assumed to be ordered ascendingly.
	 */
	else if ((desc->type == oIndexToast && O_PAGE_IS(page, LEAF)) || O_PAGE_IS(page, RIGHTMOST))
		spaceRatio = fillfactorRatio;

	result = btree_page_split_location(desc, items, targetCount, spaceRatio,
									   &split_item);

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
perform_page_split(BTreeDescr *desc, OInMemoryBlkno blkno,
				   OInMemoryBlkno new_blkno,
				   BTreeSplitItems *items,
				   OffsetNumber left_count,
				   OTuple splitkey, LocationIndex splitkey_len,
				   CommitSeqNo csn, UndoLocation undoLoc)
{
	Page		left_page = O_GET_IN_MEMORY_PAGE(blkno),
				right_page = O_GET_IN_MEMORY_PAGE(new_blkno);
	BTreePageHeader *left_header = (BTreePageHeader *) left_page,
			   *right_header = (BTreePageHeader *) right_page;
	bool		leaf = O_PAGE_IS(left_page, LEAF);
	OTuple		hikey;
	uint64		rightlink;
	LocationIndex hikeySize;

	rightlink = left_header->rightLink;
	init_new_btree_page(desc, new_blkno,
						left_header->flags & ~(O_BTREE_FLAG_LEFTMOST),
						PAGE_GET_LEVEL(left_page), false);

#ifdef ORIOLEDB_CUT_FIRST_KEY
	if (!leaf)
		items->items[left_count].size = BTreeNonLeafTuphdrSize;
#endif

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

	btree_page_reorg(desc, right_page, &items->items[left_count],
					 items->itemsCount - left_count,
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
	right_header->rightLink = rightlink;
	left_header->rightLink = MAKE_IN_MEMORY_RIGHTLINK(new_blkno,
													  O_PAGE_GET_CHANGE_COUNT(right_page));
	left_header->flags &= ~(O_BTREE_FLAG_RIGHTMOST);
	if (RightLinkIsValid(rightlink))
		O_GET_IN_MEMORY_PAGEDESC(RIGHTLINK_GET_BLKNO(rightlink))->leftBlkno = new_blkno;
	O_GET_IN_MEMORY_PAGEDESC(new_blkno)->leftBlkno = blkno;

	btree_page_reorg(desc, left_page, &items->items[0], left_count,
					 splitkey_len, splitkey, NULL);

	o_btree_page_calculate_statistics(desc, left_page);
	o_btree_page_calculate_statistics(desc, right_page);

	MARK_DIRTY(desc, blkno);
	MARK_DIRTY(desc, new_blkno);
}
