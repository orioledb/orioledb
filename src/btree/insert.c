/*-------------------------------------------------------------------------
 *
 * insert.c
 *		Routines for implementation of inserting new item into B-tree page.
 *
 * Copyright (c) 2021-2026, Oriole DB Inc.
 * Copyright (c) 2025-2026, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/btree/insert.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "orioledb.h"

#include "btree/find.h"
#include "btree/insert.h"
#include "btree/split.h"
#include "btree/page_contents.h"
#include "btree/page_chunks.h"
#include "btree/scan.h"
#include "btree/undo.h"
#include "checkpoint/checkpoint.h"
#include "recovery/recovery.h"
#include "transam/undo.h"
#include "tuple/format.h"
#include "utils/page_pool.h"
#include "utils/stopevent.h"

#include "miscadmin.h"
#include "utils/memutils.h"

/* In order to avoid use of the recursion in insert_leaf() we use context. */
typedef struct BTreeInsertStackItem
{
	/* next item in the find context. next == NULL if it's last item. */
	struct BTreeInsertStackItem *next;
	/* current find context */
	OBTreeFindPageContext *context;
	/* if level == 0, tuple is BTreeTuple else it is BTreeKey */
	OTuple		tuple;

	/*
	 * if level == 0, tupheader is BTreeLeafTuphdr else it is
	 * BTreeNonLeafTuphdr
	 */
	Pointer		tupheader;
	/* length of the tuple */
	Size		tuplen;
	/* current level of the insert */
	int			level;
	/* blkno of the right page of incomplete split. */
	OInMemoryBlkno rightBlkno;
	/* is current item replace tuple */
	bool		replace;
	/* is refind_page must be called */
	bool		refind;
} BTreeInsertStackItem;

/* Fills BTreeInsertStackItem as a downlink of current incomplete split. */
static void o_btree_split_fill_downlink_item(BTreeInsertStackItem *insert_item,
											 OInMemoryBlkno left_blkno,
											 bool lock);

/*
 * Finishes split of the rootPageBlkno page.
 * insert_item can be filled by o_btree_split_fill_downlink_item call.
 */
static OInMemoryBlkno o_btree_finish_root_split_internal(BTreeDescr *desc,
														 OInMemoryBlkno left_blkno,
														 BTreeInsertStackItem *insert_item);

/*
 * Adds a new fix split item to insert context. It modifies an insert_item.
 */
static BTreeInsertStackItem *o_btree_insert_stack_push_split_item(BTreeInsertStackItem *insert_item,
																  OInMemoryBlkno left_blkno);

static void o_btree_insert_item(BTreeInsertStackItem *insert_item,
								int reserve_kind);

/*
 * Returns true if a current page is the left page of incomplete split.
 * Should be always call before insert a new tuple to page.
 */
bool
o_btree_split_is_incomplete(OInMemoryBlkno left_blkno, uint32 pageChangeCount,
							bool *relocked)
{
	Page		p = O_GET_IN_MEMORY_PAGE(left_blkno);
	BTreePageHeader *header = (BTreePageHeader *) p;
	uint64		rightLink = header->rightLink;

	if (RightLinkIsValid(rightLink))
	{
		Page		rightP = O_GET_IN_MEMORY_PAGE(RIGHTLINK_GET_BLKNO(rightLink));

		Assert(O_PAGE_GET_CHANGE_COUNT(rightP) == RIGHTLINK_GET_CHANGECOUNT(rightLink));

		if (O_PAGE_IS(p, BROKEN_SPLIT))
			return true;

		/* wait for split finish */
		while (RightLinkIsValid(rightLink) && !O_PAGE_IS(rightP, BROKEN_SPLIT))
		{
			relock_page(left_blkno);
			*relocked = true;
			if (O_PAGE_GET_CHANGE_COUNT(p) != pageChangeCount)
				return false;

			rightLink = header->rightLink;
			if (RightLinkIsValid(rightLink))
			{
				rightP = O_GET_IN_MEMORY_PAGE(RIGHTLINK_GET_BLKNO(rightLink));
				Assert(O_PAGE_GET_CHANGE_COUNT(rightP) == RIGHTLINK_GET_CHANGECOUNT(rightLink));
			}
		}

		/* split should be broken or ok after this */
		Assert(O_PAGE_IS(rightP, BROKEN_SPLIT) || !RightLinkIsValid(rightLink));

		if (O_PAGE_IS(rightP, BROKEN_SPLIT))
			return true;
	}
	return false;
}

static void
o_btree_split_fill_downlink_item_with_key(BTreeInsertStackItem *insert_item,
										  OInMemoryBlkno left_blkno,
										  bool lock,
										  OTuple key,
										  LocationIndex keylen,
										  BTreeNonLeafTuphdr *internal_header)
{
	BTreePageHeader *header;
	OInMemoryBlkno right_blkno;
	Page		left_page = O_GET_IN_MEMORY_PAGE(left_blkno),
				right_page;

	header = (BTreePageHeader *) left_page;
	Assert(!O_PAGE_IS(left_page, RIGHTMOST));
	Assert(RightLinkIsValid(header->rightLink));

	right_blkno = RIGHTLINK_GET_BLKNO(header->rightLink);
	if (lock)
		lock_page(right_blkno);

	right_page = O_GET_IN_MEMORY_PAGE(right_blkno);
	Assert(O_PAGE_GET_CHANGE_COUNT(right_page) == RIGHTLINK_GET_CHANGECOUNT(header->rightLink));

	insert_item->tuplen = keylen;
	insert_item->tuple = key;

	internal_header->downlink = MAKE_IN_MEMORY_DOWNLINK(right_blkno,
														O_PAGE_GET_CHANGE_COUNT(right_page));

	if (lock)
		unlock_page(right_blkno);

	insert_item->tupheader = (Pointer) internal_header;
}

static void
o_btree_split_fill_downlink_item(BTreeInsertStackItem *insert_item,
								 OInMemoryBlkno left_blkno,
								 bool lock)
{
	Page		left_page = O_GET_IN_MEMORY_PAGE(left_blkno);
	OTuple		hikey;
	OTuple		key;
	LocationIndex keylen;
	BTreeNonLeafTuphdr *internal_header = palloc(sizeof(BTreeNonLeafTuphdr));

	keylen = BTREE_PAGE_GET_HIKEY_SIZE(left_page);
	BTREE_PAGE_GET_HIKEY(hikey, left_page);
	key.data = (Pointer) palloc(keylen);
	key.formatFlags = hikey.formatFlags;
	memcpy(key.data, hikey.data, keylen);

	o_btree_split_fill_downlink_item_with_key(insert_item, left_blkno, lock,
											  key, keylen, internal_header);
}

static OInMemoryBlkno
o_btree_finish_root_split_internal(BTreeDescr *desc,
								   OInMemoryBlkno left_blkno,
								   BTreeInsertStackItem *insert_item)
{
	BTreeNonLeafTuphdr internal_header;
	OrioleDBPageDesc *page_desc = O_GET_IN_MEMORY_PAGEDESC(desc->rootInfo.rootPageBlkno);
	BTreePageHeader *left_header,
			   *root_header;
	Pointer		ptr;
	Page		p = O_GET_IN_MEMORY_PAGE(desc->rootInfo.rootPageBlkno),
				left_page;
	FileExtent	root_extent = page_desc->fileExtent;
	bool		is_leaf = PAGE_GET_LEVEL(p) == 0;
	BTreePageItemLocator loc;

	left_page = O_GET_IN_MEMORY_PAGE(left_blkno);
	init_new_btree_page(desc, left_blkno, O_BTREE_FLAG_LEFTMOST, PAGE_GET_LEVEL(p), false);

	memcpy(left_page + O_PAGE_HEADER_SIZE,
		   p + O_PAGE_HEADER_SIZE,
		   ORIOLEDB_BLCKSZ - O_PAGE_HEADER_SIZE);

	page_block_reads(desc->rootInfo.rootPageBlkno);

	init_new_btree_page(desc, desc->rootInfo.rootPageBlkno,
						O_BTREE_FLAG_RIGHTMOST | O_BTREE_FLAG_LEFTMOST,
						PAGE_GET_LEVEL(left_page) + 1, true);
	init_page_first_chunk(desc, p, 0);

	/* restore checkpoint number and file offset for the rootPageBlkno */
	left_header = (BTreePageHeader *) left_page;
	root_header = (BTreePageHeader *) p;
	root_header->o_header.checkpointNum = left_header->o_header.checkpointNum;
	left_header->o_header.checkpointNum = 0;
	page_desc->fileExtent = root_extent;

	Assert(left_blkno);
	Assert(page_is_locked(desc->rootInfo.rootPageBlkno) || O_PAGE_IS_LOCAL(desc->rootInfo.rootPageBlkno));

	BTREE_PAGE_LOCATOR_FIRST(p, &loc);
	page_locator_insert_item(p, &loc, BTreeNonLeafTuphdrSize);
	BTREE_PAGE_LOCATOR_NEXT(p, &loc);
	page_locator_insert_item(p, &loc, MAXALIGN(insert_item->tuplen) + BTreeNonLeafTuphdrSize);

	ptr = BTREE_PAGE_LOCATOR_GET_ITEM(p, &loc);
	memcpy(ptr, insert_item->tupheader, BTreeNonLeafTuphdrSize);
	ptr += BTreeNonLeafTuphdrSize;
	memcpy(ptr, insert_item->tuple.data, insert_item->tuplen);
	BTREE_PAGE_SET_ITEM_FLAGS(p, &loc, insert_item->tuple.formatFlags);

	if (!(insert_item->tuple.formatFlags & O_TUPLE_FLAGS_FIXED_FORMAT))
		root_header->chunkDesc[0].chunkKeysFixed = 0;

	internal_header.downlink = MAKE_IN_MEMORY_DOWNLINK(left_blkno,
													   O_PAGE_GET_CHANGE_COUNT(left_page));
	BTREE_PAGE_LOCATOR_FIRST(p, &loc);
	ptr = BTREE_PAGE_LOCATOR_GET_ITEM(p, &loc);
	memcpy(ptr, &internal_header, BTreeNonLeafTuphdrSize);

	MARK_DIRTY(desc, left_blkno);
	MARK_DIRTY(desc, desc->rootInfo.rootPageBlkno);

	O_GET_IN_MEMORY_PAGEDESC(insert_item->rightBlkno)->leftBlkno = left_blkno;
	btree_split_mark_finished(insert_item->rightBlkno, false, true);
	insert_item->rightBlkno = OInvalidInMemoryBlkno;

	btree_page_update_max_key_len(desc, p);

	unlock_page(desc->rootInfo.rootPageBlkno);
	unlock_page(left_blkno);

	if (is_leaf)
		pg_atomic_fetch_add_u32(&BTREE_GET_META(desc)->leafPagesNum, 1);

	return left_blkno;
}

/*
 * Fixes incomplete split of a non-rootPageBlkno page.
 * Left page must be locked.  Unlocks left page and all pages used internally.
 */
static void
o_btree_fix_page_split(BTreeDescr *desc, OInMemoryBlkno left_blkno)
{
	BTreeInsertStackItem iitem;
	OBTreeFindPageContext context;
	Page		p = O_GET_IN_MEMORY_PAGE(left_blkno);
	BTreePageHeader *header = (BTreePageHeader *) p;
	BTreePageHeader *rightHeader = (BTreePageHeader *) p;
	OFixedKey	key;
	OInMemoryBlkno rightBlkno;
	int			level = PAGE_GET_LEVEL(p);

	Assert(left_blkno != desc->rootInfo.rootPageBlkno);

	iitem.context = &context;
	copy_fixed_hikey(desc, &key, p);
	rightBlkno = RIGHTLINK_GET_BLKNO(header->rightLink);
	rightHeader = (BTreePageHeader *) O_GET_IN_MEMORY_PAGE(rightBlkno);
	lock_page(rightBlkno);
	Assert(O_PAGE_IS(O_GET_IN_MEMORY_PAGE(rightBlkno), BROKEN_SPLIT));
	START_CRIT_SECTION();
	page_block_reads(rightBlkno);
	rightHeader->flags &= ~O_BTREE_FLAG_BROKEN_SPLIT;

	/*
	 * Register split.  That would put back O_BTREE_FLAG_BROKEN_SPLIT on
	 * error.
	 */
	btree_register_inprogress_split(rightBlkno);
	END_CRIT_SECTION();
	unlock_page(rightBlkno);
	unlock_page(left_blkno);

	ppool_reserve_pages(desc->ppool, PPOOL_RESERVE_FIND, 2);

	init_page_find_context(iitem.context, desc, COMMITSEQNO_INPROGRESS, BTREE_PAGE_FIND_MODIFY);

	find_page(iitem.context, &key, BTreeKeyPageHiKey, level + 1);
	iitem.rightBlkno = rightBlkno;
	iitem.replace = false;
	iitem.refind = false;
	iitem.level = level + 1;
	iitem.next = NULL;

	o_btree_split_fill_downlink_item(&iitem, left_blkno, true);
	o_btree_insert_item(&iitem, PPOOL_RESERVE_FIND);
}

/*
 * Fixes incomplete split of a page.
 * Left page must be locked. Unlocks left page and all pages used internally.
 */
void
o_btree_split_fix_and_unlock(BTreeDescr *descr, OInMemoryBlkno left_blkno)
{
	MemoryContext prev_context;
	bool		nested_call;

	nested_call = CurrentMemoryContext == btree_insert_context;
	if (!nested_call)
	{
		prev_context = MemoryContextSwitchTo(btree_insert_context);
	}

	/*
	 * Root split can't be incomplete, because it's executed within a single
	 * critical section.
	 */
	Assert(left_blkno != descr->rootInfo.rootPageBlkno);

	o_btree_fix_page_split(descr, left_blkno);

	if (!nested_call)
	{
		MemoryContextSwitchTo(prev_context);
		MemoryContextResetOnly(btree_insert_context);
	}
}

/*
 * Fixes incomplete split of a page.
 * Left page must be locked. Unlocks left page and all pages used internally.
 */
void
o_btree_split_fix_for_right_page_and_unlock(BTreeDescr *desc, OInMemoryBlkno rightBlkno)
{
	OrioleDBPageDesc *rightPageDesc = O_GET_IN_MEMORY_PAGEDESC(rightBlkno);
	OInMemoryBlkno leftBlkno;
	BTreePageHeader *leftHeader;
	uint64		rightLink;
	uint32		rightChangeCount;

	leftBlkno = rightPageDesc->leftBlkno;
	rightChangeCount = O_PAGE_GET_CHANGE_COUNT(O_GET_IN_MEMORY_PAGE(rightBlkno));

	unlock_page(rightBlkno);

	lock_page(leftBlkno);
	leftHeader = (BTreePageHeader *) O_GET_IN_MEMORY_PAGE(leftBlkno);
	rightLink = leftHeader->rightLink;

	if (RightLinkIsValid(rightLink) &&
		RIGHTLINK_GET_BLKNO(rightLink) == rightBlkno &&
		RIGHTLINK_GET_CHANGECOUNT(rightLink) == rightChangeCount)
	{
		o_btree_split_fix_and_unlock(desc, leftBlkno);
	}
	else
	{
		unlock_page(leftBlkno);
	}
}

static BTreeInsertStackItem *
o_btree_insert_stack_push_split_item(BTreeInsertStackItem *insert_item,
									 OInMemoryBlkno left_blkno)
{
	Page		p = O_GET_IN_MEMORY_PAGE(left_blkno);
	BTreePageHeader *header = (BTreePageHeader *) p;
	BTreePageHeader *rightHeader;
	BTreeInsertStackItem *new_item = palloc(sizeof(BTreeInsertStackItem));
	OInMemoryBlkno right_blkno;

	/* Should not be here. */
	Assert(insert_item->context->index != 0);

	/*
	 * The incomplete split found. We should fill a new insert item which will
	 * insert downlink to parent and push it to context.
	 */
	new_item->context = palloc(sizeof(OBTreeFindPageContext));
	*(new_item->context) = *(insert_item->context);
	new_item->context->index--;

	new_item->replace = false;
	new_item->level = insert_item->level + 1;
	new_item->next = insert_item;

	o_btree_split_fill_downlink_item(new_item, left_blkno, true);

	/* Removes broken flag and unlock page. */
	right_blkno = RIGHTLINK_GET_BLKNO(header->rightLink);
	lock_page(right_blkno);
	rightHeader = (BTreePageHeader *) O_GET_IN_MEMORY_PAGE(right_blkno);
	START_CRIT_SECTION();
	page_block_reads(right_blkno);
	rightHeader->flags &= ~O_BTREE_FLAG_BROKEN_SPLIT;
	btree_register_inprogress_split(right_blkno);
	END_CRIT_SECTION();
	unlock_page(right_blkno);
	unlock_page(left_blkno);
	insert_item->refind = true;

	new_item->rightBlkno = right_blkno;
	new_item->refind = true;

	return new_item;
}

typedef struct
{
	BTreePageItem item;
	int			index;
	int			pgprocno;
	bool		inserted;
} TupleWaiterInfo;

/*
 * Gethers information about tuples to be inserted by other processes.
 * Returns total size to be occupied by new tuples.
 */
static int
get_tuple_waiter_infos(BTreeDescr *desc,
					   int tupleWaiterProcnums[BTREE_PAGE_MAX_SPLIT_ITEMS],
					   TupleWaiterInfo tupleWaiterInfos[BTREE_PAGE_MAX_SPLIT_ITEMS],
					   int tupleWaitersCount)
{
	int			i;
	int			totalSize = 0;

	for (i = 0; i < tupleWaitersCount; i++)
	{
		OPageWaiterShmemState *lockerState = &lockerStates[tupleWaiterProcnums[i]];
		TupleWaiterInfo *tupleWaiterInfo = &tupleWaiterInfos[i];
		OTuple		tuple;

		tuple.formatFlags = lockerState->tupleFlags;
		tuple.data = &lockerState->tupleData.fixedData[BTreeLeafTuphdrSize];

		tupleWaiterInfo->item.flags = lockerState->tupleFlags;
		tupleWaiterInfo->item.data = lockerState->tupleData.fixedData;
		tupleWaiterInfo->item.size = BTreeLeafTuphdrSize +
			MAXALIGN(o_btree_len(desc,
								 tuple,
								 OTupleLength));
		tupleWaiterInfo->pgprocno = tupleWaiterProcnums[i];
		tupleWaiterInfo->index = i;
		tupleWaiterInfo->inserted = false;
		totalSize += tupleWaiterInfo->item.size;
	}

	return totalSize;
}

static int
waiter_info_cmp(const void *a, const void *b, void *arg)
{
	TupleWaiterInfo *wa = (TupleWaiterInfo *) a;
	TupleWaiterInfo *wb = (TupleWaiterInfo *) b;
	OTuple		ta;
	OTuple		tb;
	BTreeDescr *desc = (BTreeDescr *) arg;

	ta.formatFlags = wa->item.flags;
	ta.data = wa->item.data + BTreeLeafTuphdrSize;
	tb.formatFlags = wb->item.flags;
	tb.data = wb->item.data + BTreeLeafTuphdrSize;

	return o_btree_cmp(desc, &ta, BTreeKeyLeafTuple, &tb, BTreeKeyLeafTuple);

}

/*
 * Merge inputItems (existing leaf-page items plus the inserter's new tuple)
 * with the queued waiter tuples.  Walk both sequences in sort order and
 * emit the merged result into outputItems.
 *
 * The accept/reject decision for each waiter is a single global-budget gate:
 * the entire merged byte total (including locator overhead for both halves
 * of an eventual split) must fit within the combined two-page budget
 * leftSpace + rightSpace, computed under whatever maxKeyLen the candidate
 * waiter would inflate to.  All inputItems are pre-counted into the running
 * total — the original page already accommodated them, so as long as we
 * never accept a waiter that pushes the total past the combined budget,
 * inputItems are guaranteed to fit somewhere across the two output pages
 * (no per-side bookkeeping is needed during the merge — btree_page_split_
 * location() picks the actual split point afterwards).
 *
 * If a waiter's acceptance would exceed the budget, it (and every later
 * waiter) is dropped via finished = true.  Conflicting waiters (same key
 * as an input) are silently skipped.
 *
 * Returns true if the merged set doesn't fit a single page (a split is
 * required), false otherwise.
 */
static bool
merge_waited_tuples(BTreeDescr *desc, Page p, BTreeSplitItems *outputItems,
					BTreeSplitItems *inputItems,
					TupleWaiterInfo tupleWaiterInfos[BTREE_PAGE_MAX_SPLIT_ITEMS],
					int tupleWaitersCount)
{
	int			inputIndex = 0,
				outputIndex = 0,
				waitersIndex = 0;
	int			totalSize = 0,
				totalCount = 0;
	int			rightSpace,
				singlePageSpace;
	int			maxKeyLen;
	int			i;
	bool		finished = false;

	/*
	 * Stack of waiters accepted so far, in acceptance (= sort) order.  Used
	 * by the post-pass dry-run gate to drop one waiter at a time without
	 * re-walking the merge.  Each entry records where the waiter ended up in
	 * outputItems[], its index in tupleWaiterInfos[], and the aligned length
	 * of its key (so we can decide whether dropping it requires a full
	 * maxKeyLen rescan).
	 */
	struct
	{
		int			outputPos;
		int			waiterIdx;
		int			keyLen;
	}			accepted[BTREE_PAGE_MAX_SPLIT_ITEMS] = {0};
	int			acceptedTop = 0;

	outputItems->leaf = inputItems->leaf;
	outputItems->hikeySize = inputItems->hikeySize;
	outputItems->hikeysEnd = inputItems->hikeysEnd;
	outputItems->itemsCount = 0;

	/*
	 * Pre-count every input.  After the merge runs, totalSize / totalCount
	 * always include all inputs plus every accepted waiter, which makes the
	 * input-fit invariant trivial: as long as the candidate total fits the
	 * two-page budget, the inputs (a subset of total) fit by construction.
	 */
	for (i = 0; i < inputItems->itemsCount; i++)
		totalSize += inputItems->items[i].size;
	totalCount = inputItems->itemsCount;

	maxKeyLen = inputItems->maxKeyLen;

	/*
	 * The right page inherits the original page's hikey, so its budget
	 * doesn't change as the merge progresses.  singlePageSpace is the
	 * "no-split" budget — same formula as rightSpace, used at the end to
	 * decide whether the merged set fits on a single page.
	 */
	rightSpace = ORIOLEDB_BLCKSZ -
		Max(inputItems->hikeysEnd,
			MAXALIGN(sizeof(BTreePageHeader)) + inputItems->hikeySize);
	singlePageSpace = rightSpace;

	while (inputIndex < inputItems->itemsCount ||
		   (waitersIndex < tupleWaitersCount && !finished))
	{
		int			cmp;
		BTreePageItem item;
		bool		isWaiter;

		/* Pick the next item in sort order. */
		if (inputIndex >= inputItems->itemsCount)
		{
			cmp = 1;
		}
		else if (waitersIndex >= tupleWaitersCount || finished)
		{
			cmp = -1;
		}
		else
		{
			OTuple		tup1;
			OTuple		tup2;

			tup1.formatFlags = inputItems->items[inputIndex].flags;
			tup1.data = inputItems->items[inputIndex].data + BTreeLeafTuphdrSize;
			tup2.formatFlags = tupleWaiterInfos[waitersIndex].item.flags;
			tup2.data = tupleWaiterInfos[waitersIndex].item.data + BTreeLeafTuphdrSize;
			cmp = o_btree_cmp(desc,
							  &tup1, BTreeKeyLeafTuple,
							  &tup2, BTreeKeyLeafTuple);

			/* Conflicting waiter is silently dropped. */
			if (cmp == 0)
			{
				waitersIndex++;
				continue;
			}
		}

		Assert(cmp != 0);
		isWaiter = (cmp > 0);

		if (isWaiter)
		{
			OTuple		tup;
			int			newKeyLen,
						newMaxKeyLen,
						newLeftSpace,
						candidateTotalSize;

			item = tupleWaiterInfos[waitersIndex].item;
			tup.formatFlags = item.flags;
			tup.data = item.data + BTreeLeafTuphdrSize;
			newKeyLen = MAXALIGN(o_btree_len(desc, tup,
											 OTupleKeyLengthNoVersion));
			newMaxKeyLen = Max(maxKeyLen, newKeyLen);
			newLeftSpace = ORIOLEDB_BLCKSZ -
				Max(inputItems->hikeysEnd,
					MAXALIGN(sizeof(BTreePageHeader)) + newMaxKeyLen);

			candidateTotalSize = totalSize + item.size;

			/*
			 * Global budget gate.  Allow per-page locator overhead twice —
			 * once for each side of a split — as a conservative upper bound
			 * on any actual split's locator cost.  If the candidate set
			 * wouldn't fit, drop this waiter and every later one.
			 */
			if (candidateTotalSize +
				2 * MAXALIGN((totalCount + 1) * sizeof(LocationIndex)) >
				newLeftSpace + rightSpace)
			{
				finished = true;
				continue;
			}

			tupleWaiterInfos[waitersIndex].inserted = true;
			accepted[acceptedTop].outputPos = outputIndex;
			accepted[acceptedTop].waiterIdx = waitersIndex;
			accepted[acceptedTop].keyLen = newKeyLen;
			acceptedTop++;
			outputItems->items[outputIndex++] = item;
			waitersIndex++;
			totalSize = candidateTotalSize;
			totalCount++;
			maxKeyLen = newMaxKeyLen;
		}
		else
		{
			/*
			 * Inputs are pre-counted into totalSize / totalCount.  Just emit
			 * them in sort order.
			 */
			outputItems->items[outputIndex++] = inputItems->items[inputIndex++];
		}

		Assert(outputIndex < BTREE_PAGE_MAX_SPLIT_ITEMS);
	}

	outputItems->itemsCount = outputIndex;

	/*
	 * Re-derive maxKeyLen from items actually inserted; rejected waiters'
	 * wider keys must not leak into outputItems->maxKeyLen.
	 */
	outputItems->maxKeyLen = inputItems->maxKeyLen;
	for (i = 0; i < acceptedTop; i++)
		outputItems->maxKeyLen = Max(outputItems->maxKeyLen,
									 accepted[i].keyLen);

	/*
	 * Post-pass: the global-budget gate above is a conservative upper bound
	 * on what fits two pages, but it does not exactly mirror
	 * btree_page_split_location()'s loop (which is sensitive to the specific
	 * sequence of items at the boundary).  If the merged set still doesn't
	 * have a valid split, drop the most recently accepted waiter (pop the
	 * stack), shift its outputItems slot out, and re-check. Repeat until the
	 * dry-run passes — or the stack empties, in which case the inputs alone
	 * are guaranteed to be splittable since they came from a valid page.
	 */
	while (totalSize +
		   MAXALIGN(totalCount * sizeof(LocationIndex)) > singlePageSpace &&
		   !btree_page_split_can_succeed(outputItems) &&
		   acceptedTop > 0)
	{
		int			pos;
		int			waiterIdx;
		int			keyLen;
		int			itemSize;
		int			j;

		acceptedTop--;
		pos = accepted[acceptedTop].outputPos;
		waiterIdx = accepted[acceptedTop].waiterIdx;
		keyLen = accepted[acceptedTop].keyLen;
		itemSize = outputItems->items[pos].size;

		tupleWaiterInfos[waiterIdx].inserted = false;
		totalSize -= itemSize;
		totalCount--;

		for (j = pos; j < outputIndex - 1; j++)
			outputItems->items[j] = outputItems->items[j + 1];
		outputIndex--;
		outputItems->itemsCount = outputIndex;

		/*
		 * The dropped waiter's key only mattered for outputItems->maxKeyLen
		 * if it was the maximum; otherwise the existing max still bounds
		 * everything that's left.
		 */
		if (outputItems->maxKeyLen == keyLen)
		{
			outputItems->maxKeyLen = inputItems->maxKeyLen;
			for (j = 0; j < acceptedTop; j++)
				outputItems->maxKeyLen = Max(outputItems->maxKeyLen,
											 accepted[j].keyLen);
		}
	}

	/*
	 * Split is needed iff the merged set doesn't fit a single page.  When a
	 * split is needed, the post-pass above must have driven the merged set
	 * into a state that btree_page_split_location() can actually partition.
	 */
	{
		bool		splitNeeded = totalSize +
			MAXALIGN(totalCount * sizeof(LocationIndex)) > singlePageSpace;

		Assert(!splitNeeded || btree_page_split_can_succeed(outputItems));
		return splitNeeded;
	}
}

static void
o_btree_insert_mark_split_finished_if_needed(BTreeInsertStackItem *insert_item)
{
	if (insert_item->rightBlkno != OInvalidInMemoryBlkno)
	{
		btree_split_mark_finished(insert_item->rightBlkno, true, true);
		btree_unregister_inprogress_split(insert_item->rightBlkno);
		insert_item->rightBlkno = OInvalidInMemoryBlkno;
	}
}

static bool
o_btree_insert_split(BTreeInsertStackItem *insert_item,
					 BTreeSplitItems *items,
					 OffsetNumber offset,
					 CommitSeqNo csn,
					 bool needsUndo,
					 int reserve_kind,
					 int *waitersWakeupProcnums,
					 int waitersWakeupCount)
{
	OffsetNumber left_count;
	OBTreeFindPageContext *curContext = insert_item->context;
	BTreeDescr *desc = curContext->desc;
	OInMemoryBlkno blkno,
				right_blkno = OInvalidInMemoryBlkno,
				root_split_left_blkno = OInvalidInMemoryBlkno;
	Page		p;
	OTuple		split_key;
	LocationIndex split_key_len;
	UndoLocation undoLocation;
	BTreeNonLeafTuphdr *internal_header;
	bool		next;
	Jsonb	   *params = NULL;

	blkno = curContext->items[curContext->index].blkno;
	p = O_GET_IN_MEMORY_PAGE(blkno);

	if (STOPEVENTS_ENABLED())
		params = btree_page_stopevent_params(desc, p);

	left_count = btree_get_split_left_count(desc, p, offset,
											insert_item->replace,
											items,
											&split_key, &split_key_len);

	/*
	 * Make a page-level undo item if needed.
	 *
	 * A no-drop split is invisible to readers: it repartitions the same
	 * tuples across two pages without removing any.  The only reason to keep
	 * a page-level image is then to reconstruct the pre-split page structure
	 * for a reader whose snapshot predates the split; when the page's own
	 * pre-split undo location is already below the retain horizon (or
	 * invalid), no such reader exists.  In that case we write no image at all
	 * and mark both halves frozen with an invalid undo location: every reader
	 * reads them live (a frozen csn precedes any snapshot, so the undo chain
	 * is never walked) and per-tuple MVCC handles visibility, exactly as for
	 * a freshly initialized page.
	 *
	 * If the split physically drops a tuple, we must keep a full image: a
	 * tuple is droppable once it is finished for everybody (deleting xid <
	 * runXmin), but that is xid-based -- an active snapshot whose csn
	 * precedes the delete's commit still needs the tuple, and only the image
	 * preserves it.
	 *
	 * A concurrent sequential scan is the other exception: it reconstructs
	 * whole historical pages spanning the full pre-split key range through
	 * the undo chain, so it needs a full image.  Fall back to one whenever a
	 * seq scan is active on this tree.
	 */
	if (needsUndo)
	{
		BTreePageHeader *php = (BTreePageHeader *) p;
		UndoMeta   *undoMeta = get_undo_meta_by_type(GET_PAGE_LEVEL_UNDO_TYPE(desc->undoType));
		UndoLocation retainLoc = pg_atomic_read_u64(enable_rewind ?
													&undoMeta->minRewindRetainLocation :
													&undoMeta->minProcRetainLocation);
		bool		noRetainedReader = !UndoLocationIsValid(php->undoLocation) ||
			php->undoLocation < retainLoc;

		if (noRetainedReader &&
			!page_op_drops_tuple(desc, p, csn) &&
			meta_page_get_num_seq_scans(desc->rootInfo.metaPageBlkno) == 0)
		{
			csn = COMMITSEQNO_FROZEN;
			undoLocation = InvalidUndoLocation;
		}
		else
		{
			undoLocation = page_add_image_to_undo(desc, p, csn,
												  &split_key, split_key_len);
		}
	}
	else
		undoLocation = InvalidUndoLocation;

	internal_header = palloc(sizeof(BTreeNonLeafTuphdr));

	START_CRIT_SECTION();

	if (blkno == desc->rootInfo.rootPageBlkno)
		root_split_left_blkno = ppool_alloc_page(desc->ppool, reserve_kind);
	right_blkno = ppool_alloc_page(desc->ppool, reserve_kind);

	/*
	 * Move hikeyBlkno of split.  This change is atomic, no need to bother
	 * about change count.
	 */
	if (checkpoint_state->stack[insert_item->level].hikeyBlkno == blkno)
		checkpoint_state->stack[insert_item->level].hikeyBlkno = right_blkno;

	perform_page_split(desc, blkno, right_blkno, items,
					   left_count, split_key, split_key_len,
					   csn, undoLocation);

	o_btree_insert_mark_split_finished_if_needed(insert_item);

	unlock_page(right_blkno);

	if (waitersWakeupCount > 0)
		mark_waiter_tuples_inserted(waitersWakeupProcnums,
									waitersWakeupCount);

	o_btree_split_fill_downlink_item_with_key(insert_item, blkno, false,
											  split_key, split_key_len,
											  internal_header);

	if (blkno == desc->rootInfo.rootPageBlkno)
	{
		Assert(curContext->index == 0);

		insert_item->rightBlkno = right_blkno;

		blkno = o_btree_finish_root_split_internal(desc,
												   root_split_left_blkno,
												   insert_item);

		next = true;
		END_CRIT_SECTION();
	}
	else
	{
		/* node and leafs split */
		btree_register_inprogress_split(right_blkno);
		if (insert_item->level == 0)
			pg_atomic_fetch_add_u32(&BTREE_GET_META(desc)->leafPagesNum, 1);

		unlock_page_after_split(blkno);

		curContext->index--;
		insert_item->refind = true;
		next = false;
		END_CRIT_SECTION();
		insert_item->rightBlkno = right_blkno;

	}


	if (STOPEVENT_CONDITION(STOPEVENT_SPLIT_FAIL, params))
		elog(ERROR, "Debug condition: page has been split.");

	STOPEVENT(STOPEVENT_PAGE_SPLIT, params);

	if (!next)
	{
		/* Split non-rootPageBlkno case. Insert a downlink. */
		insert_item->replace = false;
		insert_item->level++;
	}

	return next;
}

static void
tuple_waiters_check_hikey(BTreeDescr *desc, Page p,
						  TupleWaiterInfo tupleWaiterInfos[BTREE_PAGE_MAX_SPLIT_ITEMS],
						  int *tupleWaitersCount)
{
	OTuple		hikey;
	int			count = (*tupleWaitersCount);

	if (O_PAGE_IS(p, RIGHTMOST))
		return;

	BTREE_PAGE_GET_HIKEY(hikey, p);

	while (count > 0)
	{
		OTuple		waiterTup;

		waiterTup.formatFlags = tupleWaiterInfos[count - 1].item.flags;
		waiterTup.data = tupleWaiterInfos[count - 1].item.data + BTreeLeafTuphdrSize;

		if (o_btree_cmp(desc,
						&waiterTup, BTreeKeyLeafTuple,
						&hikey, BTreeKeyNonLeafKey) < 0)
			break;
		count--;
	}

	(*tupleWaitersCount) = count;
}

static bool
o_btree_insert_needs_page_undo(BTreeDescr *desc, Page p)
{
	bool		needsUndo = O_PAGE_IS(p, LEAF) && desc->undoType != UndoLogNone;

	if (needsUndo && OXidIsValid(desc->createOxid) &&
		desc->createOxid == get_current_oxid_if_any())
		needsUndo = false;

	return needsUndo;
}

/*
 * Per-item pre-check shared by o_btree_insert_item_with_waiters()
 * branch and o_btree_multi_insert_item(): hikey gate, btree_page_search,
 * raw fit check, duplicate-at-locator probe.  Positions *loc on success
 * and on Duplicate (so the caller can read the conflicting tuple if it
 * wants); leaves it indeterminate on HikeyCrossed (no search was done)
 * and may leave it positioned-but-unfit on NoFit.
 *
 * key/keyType match how the caller addresses the new item: waiters pass
 * the leaf tuple itself with BTreeKeyLeafTuple, the multi-insert driver
 * passes the precomputed OBTreeKeyBound with BTreeKeyBound.
 */
static BTreeLeafProbeResult
btree_leaf_probe_insert_slot(BTreeDescr *desc, Page p, bool rightmost,
							 OTuple *hikey,
							 Pointer key, BTreeKeyType keyType,
							 LocationIndex newItemSize,
							 BTreePageItemLocator *loc)
{
	if (!rightmost &&
		o_btree_cmp(desc, hikey, BTreeKeyNonLeafKey, key, keyType) <= 0)
		return BTreeLeafProbeHikeyCrossed;

	btree_page_search(desc, p, key, keyType, NULL, loc);

	if (!page_locator_fits_new_item(p, loc, newItemSize))
		return BTreeLeafProbeNoFit;

	if (BTREE_PAGE_LOCATOR_IS_VALID(p, loc))
	{
		OTuple		existing;

		BTREE_PAGE_READ_LEAF_TUPLE(existing, p, loc);
		if (o_btree_cmp(desc, key, keyType,
						&existing, BTreeKeyLeafTuple) == 0)
			return BTreeLeafProbeDuplicate;
	}

	return BTreeLeafProbeFits;
}

/*
 * Write one new leaf item at *loc.  Shared page-write body for
 * o_btree_insert_item_with_waiters() and o_btree_multi_insert_item().
 * Caller has positioned loc, made any undo record, decided that the
 * item fits, and entered the critical section.  Caller follows
 * with optional page_split_chunk_if_needed() + MARK_DIRTY.
 */
static inline void
btree_leaf_write_new_item(BTreeDescr *desc, Page p,
						  BTreePageItemLocator *loc,
						  const BTreeLeafTuphdr *tuphdr,
						  OTuple tuple, LocationIndex tuplen)
{
	BTreePageHeader *header = (BTreePageHeader *) p;
	LocationIndex newItemSize = MAXALIGN(tuplen) + BTreeLeafTuphdrSize;
	LocationIndex keyLen;
	Pointer		ptr;

	page_locator_insert_item(p, loc, newItemSize);
	header->prevInsertOffset = BTREE_PAGE_LOCATOR_GET_OFFSET(p, loc);
	keyLen = MAXALIGN(o_btree_len(desc, tuple, OTupleKeyLengthNoVersion));
	header->maxKeyLen = Max(header->maxKeyLen, keyLen);

	ptr = BTREE_PAGE_LOCATOR_GET_ITEM(p, loc);
	memcpy(ptr, tuphdr, BTreeLeafTuphdrSize);
	ptr += BTreeLeafTuphdrSize;
	memcpy(ptr, tuple.data, tuplen);
	BTREE_PAGE_SET_ITEM_FLAGS(p, loc, tuple.formatFlags);

	if (!(tuple.formatFlags & O_TUPLE_FLAGS_FIXED_FORMAT))
		header->chunkDesc[loc->chunkOffset].chunkKeysFixed = 0;
}

/*
 * Insert a strict prefix of items[0..nitems-1] into the leaf ctx is parked
 * on, in one lock cycle.  Returns N, the count inserted:
 *   items[0..N-1]        inserted, in order.
 *   items[N]             the item that bailed; *result says why
 *                        (HikeyCrossed / NoFit / Duplicate).
 *   items[N+1..nitems-1] untouched.
 * N == nitems means the whole batch fit; *result is left at Fits.
 *
 * The leaf is always unlocked before return.
 *
 * Caller is responsible for:
 *  - ctx points at a LEAF, locked exclusive, no incomplete split
 *  - desc->ppool->numPagesReserved[PPOOL_RESERVE_INSERT] >= 2
 *  - keys[] ascending, tuples[] are the matching leaf tuples
 *  - Reserving enough row-undo for every per-row make_undo_record
 *    plus one extra row of slack for get_undo_record's buffer-wrap retry,
 *    so no eviction happens with the leaf locked
 */
int
o_btree_multi_insert_item(OBTreeFindPageContext *ctx,
						  OTuple *tuples, LocationIndex *tuplens,
						  Pointer *keys, BTreeKeyType keyType,
						  int nitems,
						  OXid opOxid, RowLockMode lockMode,
						  BTreeModifyCallbackInfo *cb,
						  void **cb_args,
						  BTreeLeafProbeResult *result)
{
	BTreeDescr *desc = ctx->desc;
	OInMemoryBlkno blkno;
	Page		p;
	BTreePageItemLocator loc;
	OTupleXactInfo xactInfo;
	OTuple		hikey;
	bool		rightmost;
	bool		needsUndo;
	int			inserted = 0;
	int			k;

	Assert(ctx->index >= 0 && ctx->index < ORIOLEDB_MAX_DEPTH);
	blkno = ctx->items[ctx->index].blkno;
	p = O_GET_IN_MEMORY_PAGE(blkno);
	Assert(O_PAGE_IS(p, LEAF));
	Assert(desc->ppool->numPagesReserved[PPOOL_RESERVE_INSERT] >= 2);

	rightmost = O_PAGE_IS(p, RIGHTMOST);
	if (!rightmost)
		BTREE_PAGE_GET_HIKEY(hikey, p);

	xactInfo = OXID_GET_XACT_INFO(OXidIsValid(opOxid) ? opOxid : BootstrapTransactionId,
								  lockMode, false);

	/*
	 * Mirrors the needsUndo logic in o_btree_modify_internal (self-created
	 * shortcut).
	 */
	needsUndo = desc->undoType != UndoLogNone;
	if (!(cb && cb->needsUndoForSelfCreated) &&
		OXidIsValid(desc->createOxid) &&
		desc->createOxid == opOxid &&
		!UndoLocationIsValid(get_subxact_undo_location(desc->undoType)))
		needsUndo = false;

	page_block_reads(blkno);

	*result = BTreeLeafProbeFits;

	for (k = 0; k < nitems; k++)
	{
		OTuple		tuple = tuples[k];
		LocationIndex tuplen = tuplens[k];
		LocationIndex newItemSize = MAXALIGN(tuplen) + BTreeLeafTuphdrSize;
		BTreeLeafTuphdr tuphdr;
		UndoLocation undoLocation = InvalidUndoLocation;

		*result = btree_leaf_probe_insert_slot(desc, p, rightmost, &hikey,
											   keys[k], keyType, newItemSize, &loc);
		if (*result != BTreeLeafProbeFits)
		{
			/*
			 * Items are sorted: HikeyCrossed means every remaining key is
			 * also past, so the outer driver re-finds the next leaf.  NoFit
			 * and Duplicate are bailed-to-slow-path conditions.
			 */
			break;
		}

		/*
		 * Build per-row undo record + tuphdr.  Matches the non-replace branch
		 * of o_btree_modify_add_undo_record: real undo record goes on the
		 * undo stack via make_undo_record(), tuphdr.undoLocation in the page
		 * only carries the command-tag for UndoLogRegular.
		 */
		if (needsUndo)
			undoLocation = make_undo_record(desc, tuple, true,
											BTreeOperationInsert, blkno,
											O_PAGE_GET_CHANGE_COUNT(p), NULL);

		tuphdr.xactInfo = xactInfo;
		tuphdr.deleted = BTreeLeafTupleNonDeleted;
		tuphdr.chainHasLocks = false;
		tuphdr.formatFlags = 0;
		tuphdr.undoLocation = InvalidUndoLocation;
		if (desc->undoType == UndoLogRegular && !is_recovery_process())
			tuphdr.undoLocation |= current_command_get_undo_location();

		START_CRIT_SECTION();

		btree_leaf_write_new_item(desc, p, &loc, &tuphdr, tuple, tuplen);
		page_split_chunk_if_needed(desc, p, &loc);
		MARK_DIRTY(desc, blkno);

		/*
		 * Fire the post-undo hook under the page lock so the pendingSkUndoLoc
		 * marker is installed before the next iteration advances the page --
		 * matches the ordering in o_btree_modify_insert_update().
		 */
		if (cb && cb->postUndoRecorded)
			cb->postUndoRecorded(needsUndo ? undoLocation : WaitingSkUndoLoc,
								 cb_args ? cb_args[k] : cb->arg);

		END_CRIT_SECTION();

		inserted++;
	}

	unlock_page(blkno);
	return inserted;
}

static bool
o_btree_insert_item_with_waiters(BTreeInsertStackItem *insert_item,
								 int reserve_kind,
								 int tupleWaiterProcnums[BTREE_PAGE_MAX_SPLIT_ITEMS],
								 int tupleWaitersCount)
{
	BTreeDescr *desc = insert_item->context->desc;
	OBTreeFindPageContext *curContext = insert_item->context;
	BTreeSplitItems items;
	BTreeSplitItems newItems;
	int			i,
				waitersWakeupCount = 0;
	CommitSeqNo csn;
	bool		needsUndo;
	OffsetNumber offset;
	bool		split;
	OInMemoryBlkno blkno;
	TupleWaiterInfo tupleWaiterInfos[BTREE_PAGE_MAX_SPLIT_ITEMS];
	Page		p;
	int			totalSize;
	BTreePageItemLocator loc;

	totalSize = get_tuple_waiter_infos(desc,
									   tupleWaiterProcnums,
									   tupleWaiterInfos,
									   tupleWaitersCount);

	blkno = curContext->items[curContext->index].blkno;
	Assert(OInMemoryBlknoIsValid(blkno));
	p = O_GET_IN_MEMORY_PAGE(blkno);

	if (tupleWaitersCount <= BTREE_PAGE_ITEMS_COUNT(p) &&
		MAXALIGN(insert_item->tuplen) + BTreeLeafTuphdrSize +
		totalSize + MAXALIGN(sizeof(LocationIndex)) * (tupleWaitersCount + 1) <=
		BTREE_PAGE_FREE_SPACE(p))
	{
		bool		rightmost = O_PAGE_IS(p, RIGHTMOST);
		OTuple		hikey;

		if (!rightmost)
			hikey = page_get_hikey(p);

		page_block_reads(blkno);

		for (i = 0; i <= tupleWaitersCount; i++)
		{
			LocationIndex tuplen;
			BTreeLeafTuphdr tuphdr;
			OTuple		tuple;

			if (i == 0)
			{
				loc = curContext->items[curContext->index].locator;
				tuple = insert_item->tuple;
				tuplen = insert_item->tuplen;
				tuphdr = *((BTreeLeafTuphdr *) insert_item->tupheader);
				START_CRIT_SECTION();
			}
			else
			{
				TupleWaiterInfo *waiterInfo = &tupleWaiterInfos[i - 1];
				OPageWaiterShmemState *lockerState = &lockerStates[waiterInfo->pgprocno];
				BTreeLeafProbeResult result;

				tuple.formatFlags = waiterInfo->item.flags;
				tuple.data = waiterInfo->item.data + BTreeLeafTuphdrSize;
				tuphdr = *((BTreeLeafTuphdr *) waiterInfo->item.data);
				tuplen = waiterInfo->item.size - BTreeLeafTuphdrSize;

				result = btree_leaf_probe_insert_slot(desc, p, rightmost, &hikey,
													  (Pointer) &tuple, BTreeKeyLeafTuple,
													  waiterInfo->item.size, &loc);

				/*
				 * Waiters arrive in arrival order, not sorted: a stray past
				 * the hikey or a duplicate doesn't imply later waiters fail
				 * too, so skip and keep trying.  A non-fit means the page is
				 * out of slack for the remaining items -- abandon them.
				 */
				if (result == BTreeLeafProbeHikeyCrossed ||
					result == BTreeLeafProbeDuplicate)
					continue;
				if (result == BTreeLeafProbeNoFit)
					break;

				START_CRIT_SECTION();
				if (desc->undoType != UndoLogNone)
				{
					steal_reserved_undo_size(desc->undoType,
											 lockerState->reservedUndoSize);
					make_waiter_undo_record(desc, blkno,
											waiterInfo->pgprocno,
											lockerState);
					tuphdr.undoLocation = InvalidUndoLocation | lockerState->undoLocation;
				}
				lockerState->inserted = true;
			}

			btree_leaf_write_new_item(desc, p, &loc, &tuphdr, tuple, tuplen);
			MARK_DIRTY(desc, blkno);
			END_CRIT_SECTION();
		}

		unlock_page(blkno);
		return true;
	}

	qsort_arg(tupleWaiterInfos,
			  tupleWaitersCount,
			  sizeof(TupleWaiterInfo),
			  waiter_info_cmp,
			  desc);

	tuple_waiters_check_hikey(desc, p,
							  tupleWaiterInfos,
							  &tupleWaitersCount);

	loc = curContext->items[curContext->index].locator;
	offset = BTREE_PAGE_LOCATOR_GET_OFFSET(p, &loc);

	needsUndo = o_btree_insert_needs_page_undo(desc, p);

	/* Get CSN for undo item if needed */
	if (needsUndo)
		csn = pg_atomic_fetch_add_u64(&TRANSAM_VARIABLES->nextCommitSeqNo, 1);
	else
		csn = COMMITSEQNO_INPROGRESS;

	make_split_items(desc, p, &items, &offset,
					 insert_item->tupheader,
					 insert_item->tuple,
					 insert_item->tuplen,
					 insert_item->replace,
					 csn);

	split = merge_waited_tuples(desc, p, &newItems, &items,
								tupleWaiterInfos,
								tupleWaitersCount);

	for (i = 0; i < tupleWaitersCount; i++)
	{
		if (tupleWaiterInfos[i].inserted)
		{
			OPageWaiterShmemState *lockerState = &lockerStates[tupleWaiterInfos[i].pgprocno];

			tupleWaiterProcnums[waitersWakeupCount++] = tupleWaiterInfos[i].pgprocno;

			if (desc->undoType != UndoLogNone)
			{
				BTreeLeafTuphdr *tuphdr;

				steal_reserved_undo_size(desc->undoType,
										 lockerState->reservedUndoSize);
				make_waiter_undo_record(desc, blkno,
										tupleWaiterInfos[i].pgprocno,
										lockerState);
				tuphdr = (BTreeLeafTuphdr *) tupleWaiterInfos[i].item.data;
				tuphdr->undoLocation = InvalidUndoLocation | lockerState->undoLocation;
			}
		}
	}

	Assert(items.itemsCount + waitersWakeupCount == newItems.itemsCount);

	if (!split)
	{
		START_CRIT_SECTION();
		perform_page_compaction(desc, blkno, &newItems, needsUndo, csn);
		MARK_DIRTY(desc, blkno);

		if (waitersWakeupCount > 0)
			mark_waiter_tuples_inserted(tupleWaiterProcnums,
										waitersWakeupCount);

		o_btree_insert_mark_split_finished_if_needed(insert_item);
		unlock_page(blkno);
		END_CRIT_SECTION();
		return true;
	}
	else
	{
		return o_btree_insert_split(insert_item, &newItems, offset, csn,
									needsUndo, reserve_kind,
									tupleWaiterProcnums,
									waitersWakeupCount);
	}
}

static bool
o_btree_insert_item_no_waiters(BTreeInsertStackItem *insert_item,
							   int reserve_kind)
{
	BTreeDescr *desc = insert_item->context->desc;
	OBTreeFindPageContext *curContext = insert_item->context;
	OInMemoryBlkno blkno;
	LocationIndex tupheaderlen;
	LocationIndex newItemSize;
	BTreePageItemLocator loc;
	Page		p;
	BTreeItemPageFitType fit;
	BTreePageHeader *header;

	blkno = curContext->items[curContext->index].blkno;
	loc = curContext->items[curContext->index].locator;
	tupheaderlen = (insert_item->level > 0) ?
		BTreeNonLeafTuphdrSize : BTreeLeafTuphdrSize;

	Assert(OInMemoryBlknoIsValid(blkno));
	p = O_GET_IN_MEMORY_PAGE(blkno);
	header = (BTreePageHeader *) p;
	newItemSize = MAXALIGN(insert_item->tuplen) + tupheaderlen;

	/*
	 * Pass the current value of nextCommitSeqNo to page_locator_fits_item().
	 * The result could be somewhat pessimistic: it might happen that we could
	 * actually compact more due to advance of nextCommitSeqNo.
	 */
	fit = page_locator_fits_item(desc,
								 p,
								 &loc,
								 newItemSize,
								 insert_item->replace,
								 pg_atomic_read_u64(&TRANSAM_VARIABLES->nextCommitSeqNo));

	if (fit == BTreeItemPageFitAsIs)
	{
		Pointer		ptr;

		START_CRIT_SECTION();
		page_block_reads(blkno);

		if (!insert_item->replace)
		{
			LocationIndex keyLen;

			page_locator_insert_item(p, &loc, newItemSize);
			header->prevInsertOffset = BTREE_PAGE_LOCATOR_GET_OFFSET(p, &loc);

			if (O_PAGE_IS(p, LEAF))
				keyLen = MAXALIGN(o_btree_len(desc, insert_item->tuple, OTupleKeyLengthNoVersion));
			else
				keyLen = MAXALIGN(insert_item->tuplen);
			header->maxKeyLen = Max(header->maxKeyLen, keyLen);
		}
		else
		{
			int			prevItemSize;
			BTreeLeafTuphdr prev;

			prev = *((BTreeLeafTuphdr *) BTREE_PAGE_LOCATOR_GET_ITEM(p, &loc));
			prevItemSize = BTREE_PAGE_GET_ITEM_SIZE(p, &loc);
			Assert(O_PAGE_IS(p, LEAF));

			if (!prev.deleted)
			{
				OTuple		tuple;

				BTREE_PAGE_READ_TUPLE(tuple, p, &loc);
				PAGE_ADD_N_VACATED(p, BTreeLeafTuphdrSize + MAXALIGN(o_btree_len(desc, tuple, OTupleLength)));
			}

			/*
			 * If new tuple is less then previous one, don't resize page item
			 * immediately.  We want to be able to rollback this action
			 * without page splits.
			 *
			 * Page compaction will re-use unoccupied page space when needed.
			 */
			if (newItemSize > prevItemSize)
			{
				page_locator_resize_item(p, &loc, newItemSize);
				PAGE_SUB_N_VACATED(p, prevItemSize);
				header->prevInsertOffset = BTREE_PAGE_LOCATOR_GET_OFFSET(p, &loc);
			}
			else
			{
				OTuple		tuple pg_attribute_unused();

				BTREE_PAGE_READ_TUPLE(tuple, p, &loc);
				PAGE_SUB_N_VACATED(p, BTreeLeafTuphdrSize +
								   MAXALIGN(insert_item->tuplen));
				header->prevInsertOffset = MaxOffsetNumber;
			}

			/*
			 * We replace tuples only in leafs.  Only inserts go to the
			 * non-leaf pages.
			 */
			Assert(insert_item->level == 0);
		}

		/* Copy new tuple and header */
		ptr = BTREE_PAGE_LOCATOR_GET_ITEM(p, &loc);
		memcpy(ptr, insert_item->tupheader, tupheaderlen);
		ptr += tupheaderlen;
		memcpy(ptr, insert_item->tuple.data, insert_item->tuplen);
		BTREE_PAGE_SET_ITEM_FLAGS(p, &loc, insert_item->tuple.formatFlags);

		if (!(insert_item->tuple.formatFlags & O_TUPLE_FLAGS_FIXED_FORMAT))
			header->chunkDesc[loc.chunkOffset].chunkKeysFixed = 0;

		page_split_chunk_if_needed(desc, p, &loc);

		MARK_DIRTY(desc, blkno);

		o_btree_insert_mark_split_finished_if_needed(insert_item);
		unlock_page(blkno);

		END_CRIT_SECTION();

		return true;
	}
	else
	{
		BTreeSplitItems items;
		OffsetNumber offset;
		CommitSeqNo csn;
		bool		needsUndo;

		/*
		 * No compaction should occur for bridge index: we need to keep the
		 * entries for VACUUM.
		 */
		Assert(fit == BTreeItemPageFitSplitRequired ||
			   desc->type != oIndexBridge);

		offset = BTREE_PAGE_LOCATOR_GET_OFFSET(p, &loc);

		/* Get CSN for undo item if needed */
		needsUndo = o_btree_insert_needs_page_undo(desc, p);
		if (needsUndo)
			csn = pg_atomic_fetch_add_u64(&TRANSAM_VARIABLES->nextCommitSeqNo, 1);
		else
			csn = COMMITSEQNO_INPROGRESS;

		make_split_items(desc, p, &items, &offset,
						 insert_item->tupheader,
						 insert_item->tuple,
						 insert_item->tuplen,
						 insert_item->replace,
						 csn);

		/*
		 * After make_split_items() reclaims deleted tuples, the remaining
		 * items may fit on a single page even if page_locator_fits_item()
		 * estimated a split was needed.  Check actual total size and do
		 * compaction instead of split when possible.
		 */
		if (fit == BTreeItemPageFitCompactRequired ||
			(O_PAGE_IS(p, LEAF) && split_items_fit_single_page(&items)))
		{
			START_CRIT_SECTION();

			perform_page_compaction(desc, blkno, &items, needsUndo, csn);
			header->prevInsertOffset = offset;

			MARK_DIRTY(desc, blkno);
			o_btree_insert_mark_split_finished_if_needed(insert_item);
			unlock_page(blkno);

			END_CRIT_SECTION();

			return true;
		}

		return o_btree_insert_split(insert_item, &items, offset, csn,
									needsUndo, reserve_kind, NULL, 0);
	}
}

static void
o_btree_insert_item(BTreeInsertStackItem *insert_item, int reserve_kind)
{
	BTreeKeyType kind;
	BTreeDescr *desc = insert_item->context->desc;
	OInMemoryBlkno blkno = OInvalidInMemoryBlkno;

	Assert(insert_item != NULL);

	/*--
	 * Guarantees that we never have recursive calls of o_btree_insert_item() such
	 * as:
	 * o_btree_insert_item()->refind_page()->find_page()
	 *							      ->o_btree_fix_page_split()->o_btree_insert_item()
	 *
	 * Reasons:
	 *
	 * 1. o_btree_insert_item() algorithm fixes broken splits itself for pages
	 *    founded by refind_page().
	 * 2. Inner call of ppool_reserve_pages(kind, 2) with a same kind is
	 *    incorrect.
	 */
	Assert(!(insert_item->context->flags & BTREE_PAGE_FIND_FIX_LEAF_SPLIT));

	while (insert_item != NULL)
	{
		OBTreeFindPageContext *curContext = insert_item->context;
		bool		next = false;
		int			tupleWaiterProcnums[BTREE_PAGE_MAX_SPLIT_ITEMS];
		int			tupleWaitersCount;

		Assert(desc->ppool->numPagesReserved[reserve_kind] >= 2);

		if (insert_item->level > 0)
			kind = BTreeKeyNonLeafKey;
		else
			kind = BTreeKeyLeafTuple;

		if (insert_item->level == 0)
		{

			Assert(curContext->index >= 0 && curContext->index < ORIOLEDB_MAX_DEPTH);
			blkno = curContext->items[curContext->index].blkno;

			/*
			 * it can be called only from o_btree_insert_tuple_to_leaf()
			 * o_btree_insert_tuple_to_leaf() can be called only from
			 * o_btree_normal_modify()
			 */

			/*
			 * we already make incomplete split checks in (re)find_page()
			 * inside o_btree_normal_modify().
			 */
			Assert(insert_item->refind == false);
		}
		else
		{
			bool		relocked = false;
			uint32		pageChangeCount;

			if (insert_item->refind)
			{
				OFindPageResult result PG_USED_FOR_ASSERTS_ONLY;

				/*
				 * Re-find appropriate tree page.  It might happen that parent
				 * page is not available in context.  That may happen due to
				 * concurrent rootPageBlkno split or page location using hint.
				 * Then just find appropriate page from the rootPageBlkno.
				 */
				BTREE_PAGE_FIND_UNSET(curContext, IMAGE);
				if (curContext->index >= 0)
					result = refind_page(curContext, &insert_item->tuple, kind,
										 insert_item->level,
										 curContext->items[curContext->index].blkno,
										 curContext->items[curContext->index].pageChangeCount);
				else
					result = find_page(curContext, &insert_item->tuple, kind,
									   insert_item->level);
				Assert(result == OFindPageResultSuccess);
				insert_item->refind = false;
			}

			Assert(curContext->index >= 0 && curContext->index < ORIOLEDB_MAX_DEPTH);
			blkno = curContext->items[curContext->index].blkno;
			pageChangeCount = curContext->items[curContext->index].pageChangeCount;

			if (o_btree_split_is_incomplete(blkno,
											pageChangeCount,
											&relocked))
			{
				/* pushes fix split item to the insert context */
				insert_item = o_btree_insert_stack_push_split_item(insert_item,
																   blkno);
				continue;
			}
			else if (relocked)
			{
				/* page is changed, we should refind current tuple */
				unlock_page(blkno);
				insert_item->refind = true;
				continue;
			}
		}

		Assert(OInMemoryBlknoIsValid(blkno));

		if (insert_item->level > 0 &&
			page_is_under_checkpoint(desc, blkno, false))
		{
			/*
			 * We change a node that is under checkpoint and must mark it as
			 * autonomous.
			 */
			backend_set_autonomous_level(checkpoint_state, insert_item->level);
		}

		if (insert_item->level == 0 && !insert_item->replace)
		{
			if (STOPEVENTS_ENABLED())
			{
				Page		page = O_GET_IN_MEMORY_PAGE(blkno);
				Jsonb	   *params;

				params = btree_page_stopevent_params(desc, page);
				STOPEVENT(STOPEVENT_BEFORE_GET_WAITERS_WITH_TUPLES, params);
			}

			tupleWaitersCount = get_waiters_with_tuples(desc, blkno, tupleWaiterProcnums);
		}
		else
			tupleWaitersCount = 0;

		if (tupleWaitersCount > 0)
			next = o_btree_insert_item_with_waiters(insert_item,
													reserve_kind,
													tupleWaiterProcnums,
													tupleWaitersCount);
		else
			next = o_btree_insert_item_no_waiters(insert_item,
												  reserve_kind);

		if (next)
			insert_item = insert_item->next;

		if (insert_item != NULL)
			ppool_reserve_pages(desc->ppool, reserve_kind, 2);
	}
	ppool_release_reserved(desc->ppool, PPOOL_KIND_GET_MASK(reserve_kind));
}

void
o_btree_insert_tuple_to_leaf(OBTreeFindPageContext *context,
							 OTuple tuple, LocationIndex tuplen,
							 BTreeLeafTuphdr *tuphdr, bool replace,
							 int reserve_kind)
{
	BTreeInsertStackItem insert_item;
	MemoryContext prev_context;
	bool		nested_call;

	nested_call = CurrentMemoryContext == btree_insert_context;
	if (!nested_call)
		prev_context = MemoryContextSwitchTo(btree_insert_context);

	context->flags &= ~(BTREE_PAGE_FIND_FIX_LEAF_SPLIT);
	insert_item.next = NULL;
	insert_item.context = context;
	insert_item.tuple = tuple;
	insert_item.tuplen = tuplen;
	insert_item.tupheader = (Pointer) tuphdr;
	insert_item.level = 0;
	insert_item.replace = replace;
	insert_item.rightBlkno = OInvalidInMemoryBlkno;
	insert_item.refind = false;

	o_btree_insert_item(&insert_item, reserve_kind);

	if (!nested_call)
	{
		MemoryContextSwitchTo(prev_context);
		MemoryContextResetOnly(btree_insert_context);
	}
}
