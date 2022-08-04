/*-------------------------------------------------------------------------
 *
 * page_chunks.c
 *		Internals of OrioleDB page chunks: routines for working with chunks
 *		and their items.
 *
 * Copyright (c) 2021-2022, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/btree/page_chunks.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "orioledb.h"

#include "btree/find.h"
#include "btree/insert.h"
#include "btree/page_chunks.h"
#include "btree/undo.h"
#include "recovery/recovery.h"
#include "transam/undo.h"
#include "utils/page_pool.h"
#include "utils/ucm.h"

#include "access/transam.h"
#include "miscadmin.h"
#include "utils/memdebug.h"

static void reclaim_page_space(BTreeDescr *desc, Pointer p, CommitSeqNo csn,
							   BTreePageItemLocator *location,
							   OTuple tuple, LocationIndex tuplesize,
							   bool replace);

/*
 * Load chunk to the partial page.
 */
bool
partial_load_chunk(PartialPageState *partial, Page img, OffsetNumber chunkOffset)
{
	uint32		imgState,
				srcState;
	Page		src = partial->src;
	LocationIndex chunkBegin,
				chunkEnd;
	BTreePageHeader *header = (BTreePageHeader *) img;

	if (!partial->isPartial || partial->chunkIsLoaded[chunkOffset])
		return true;

	chunkBegin = SHORT_GET_LOCATION(header->chunkDesc[chunkOffset].shortLocation);
	if (chunkOffset + 1 < header->chunksCount)
		chunkEnd = SHORT_GET_LOCATION(header->chunkDesc[chunkOffset + 1].shortLocation);
	else
		chunkEnd = header->dataSize;

	Assert(chunkBegin >= 0 && chunkBegin <= ORIOLEDB_BLCKSZ);
	Assert(chunkEnd >= 0 && chunkEnd <= ORIOLEDB_BLCKSZ);

	memcpy((Pointer) img + chunkBegin,
		   (Pointer) src + chunkBegin,
		   chunkEnd - chunkBegin);

	pg_read_barrier();

	imgState = pg_atomic_read_u32(&(O_PAGE_HEADER(img)->state));
	srcState = pg_atomic_read_u32(&(O_PAGE_HEADER(src)->state));
	if ((imgState & PAGE_STATE_CHANGE_COUNT_MASK) != (srcState & PAGE_STATE_CHANGE_COUNT_MASK) ||
		O_PAGE_STATE_READ_IS_BLOCKED(srcState))
		return false;

	if (O_PAGE_GET_CHANGE_COUNT(img) != O_PAGE_GET_CHANGE_COUNT(src))
		return false;

	partial->chunkIsLoaded[chunkOffset] = true;
	return true;
}

BTreeItemPageFitType
page_locator_fits_item(BTreeDescr *desc, Page p, BTreePageItemLocator *locator,
					   LocationIndex size, bool replace, CommitSeqNo csn)
{
	int			freeSpace = BTREE_PAGE_FREE_SPACE(p);
	int			spaceNeeded = size;
	LocationIndex oldItemSize = 0;

	Assert(spaceNeeded == MAXALIGN(spaceNeeded));

	if (!replace)
	{
		/*
		 * During insert of new item, take into account extension of chunk
		 * items array.
		 */
		spaceNeeded +=
			MAXALIGN((locator->chunkItemsCount + 1) * sizeof(LocationIndex)) -
			MAXALIGN(locator->chunkItemsCount * sizeof(LocationIndex));
	}
	else
	{
		oldItemSize = BTREE_PAGE_GET_ITEM_SIZE(p, locator);

		/* We can replace tuple only on leafs */
		Assert(O_PAGE_IS(p, LEAF));

		spaceNeeded -= oldItemSize;
		Assert(spaceNeeded == MAXALIGN(spaceNeeded));
	}

	if (freeSpace >= spaceNeeded)
	{
		/* Already have enough of free space on the page */
		return BTreeItemPageFitAsIs;
	}
	else if (O_PAGE_IS(p, LEAF))
	{
		/* Start with optimistic estimate of free space after compaction */
		int			compactedFreeSpace = freeSpace + PAGE_GET_N_VACATED(p);

		if (replace)
		{
			BTreeLeafTuphdr *tupHdr;

			tupHdr = (BTreeLeafTuphdr *) BTREE_PAGE_LOCATOR_GET_ITEM(p, locator);
			if (tupHdr->deleted &&
				XACT_INFO_FINISHED_FOR_EVERYBODY(tupHdr->xactInfo))
			{
				Assert(COMMITSEQNO_IS_INPROGRESS(csn) ||
					   XACT_INFO_MAP_CSN(tupHdr->xactInfo) < csn);

				/*
				 * Evade double calculation of space occupied by item to be
				 * replace, which will go away after compaction.
				 */
				compactedFreeSpace -= oldItemSize;
			}
		}

		/*
		 * We have a chance to do a compation on leaf.  Check if at least
		 * optimistic esimate will work.
		 */
		if (compactedFreeSpace < spaceNeeded)
			return BTreeItemPageFitSplitRequired;

		/*
		 * Switch to real estimate.  Real estimate is much slower, but there
		 * is a good chance to evade a page split.
		 */
		compactedFreeSpace -= PAGE_GET_N_VACATED(p) - page_get_vacated_space(desc, p, csn);
		if (compactedFreeSpace >= spaceNeeded)
			return BTreeItemPageFitCompactRequired;
		else
			return BTreeItemPageFitSplitRequired;
	}
	else
	{
		return BTreeItemPageFitSplitRequired;
	}
}

void
perform_page_compaction(BTreeDescr *desc, OInMemoryBlkno blkno,
						BTreePageItemLocator *loc, OTuple tuple,
						LocationIndex tuplesize, bool replace)
{
	CommitSeqNo csn;
	Page		p = O_GET_IN_MEMORY_PAGE(blkno);
	BTreePageHeader *header = (BTreePageHeader *) p;
	UndoLocation undoLocation;

	START_CRIT_SECTION();

	Assert(O_PAGE_IS(p, LEAF));

	/* Make a page-level undo item if needed */
	if (desc->undoType != UndoReserveNone)
	{
		csn = pg_atomic_fetch_add_u64(&ShmemVariableCache->nextCommitSeqNo, 1);
		undoLocation = page_add_item_to_undo(desc, p, csn, NULL, 0);

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
		csn = COMMITSEQNO_INPROGRESS;
	}

	reclaim_page_space(desc, p, csn, loc, tuple, tuplesize, replace);
	Assert(header->dataSize <= ORIOLEDB_BLCKSZ);

	END_CRIT_SECTION();
}

/*
 * Reclaim page space occupied by deleted and/or resized items.
 */
static void
reclaim_page_space(BTreeDescr *desc, Pointer p, CommitSeqNo csn,
				   BTreePageItemLocator *location,
				   OTuple tuple, LocationIndex tuplesize,
				   bool replace)
{
	BTreePageItemLocator loc;
	BTreePageItem items[BTREE_PAGE_MAX_CHUNK_ITEMS];
	int			i = 0;
	OFixedKey	hikey;
	LocationIndex hikeySize,
				nVacated = 0;
	bool		addedNewItem = false;

	Assert(O_PAGE_IS(p, LEAF));

	/*
	 * Iterate page items and check if they can be erased or truncated.
	 */
	BTREE_PAGE_FOREACH_ITEMS(p, &loc)
	{
		BTreeLeafTuphdr *tupHdr;
		OTuple		tup;
		bool		finished;

		if (!addedNewItem &&
			((loc.chunkOffset == location->chunkOffset &&
			  loc.itemOffset == location->itemOffset) || loc.chunkOffset > location->chunkOffset))
		{
			items[i].data = tuple.data;
			items[i].flags = tuple.formatFlags;
			items[i].size = tuplesize;
			Assert(items[i].size <= BTreeLeafTuphdrSize + O_BTREE_MAX_TUPLE_SIZE);
			items[i].newItem = true;
			i++;
			addedNewItem = true;
			if (replace)
			{
				Assert(loc.chunkOffset == location->chunkOffset && loc.itemOffset == location->itemOffset);
				continue;
			}
		}

		BTREE_PAGE_READ_LEAF_ITEM(tupHdr, tup, p, &loc);
		finished = XACT_INFO_FINISHED_FOR_EVERYBODY(tupHdr->xactInfo);
		if (finished && tupHdr->deleted &&
			(COMMITSEQNO_IS_INPROGRESS(csn) || XACT_INFO_MAP_CSN(tupHdr->xactInfo) < csn))
		{
			continue;
		}

		items[i].data = (Pointer) tupHdr;
		items[i].flags = tup.formatFlags;
		items[i].size = finished ?
			(BTreeLeafTuphdrSize + MAXALIGN(o_btree_len(desc, tup, OTupleLength))) :
			BTREE_PAGE_GET_ITEM_SIZE(p, &loc);
		Assert(items[i].size <= BTreeLeafTuphdrSize + O_BTREE_MAX_TUPLE_SIZE);
		items[i].newItem = false;

		if (tupHdr->deleted)
			nVacated += BTREE_PAGE_GET_ITEM_SIZE(p, &loc);
		else if (!finished)
			nVacated += BTREE_PAGE_GET_ITEM_SIZE(p, &loc) -
				(BTreeLeafTuphdrSize + MAXALIGN(o_btree_len(desc, tup, OTupleLength)));

		i++;
	}

	if (!addedNewItem)
	{
		items[i].data = tuple.data;
		items[i].flags = tuple.formatFlags;
		items[i].size = tuplesize;
		Assert(items[i].size <= BTreeLeafTuphdrSize + O_BTREE_MAX_TUPLE_SIZE);
		items[i].newItem = true;
		i++;
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
	btree_page_reorg(desc, p, items, i, hikeySize, hikey.tuple, location);
	PAGE_SET_N_VACATED(p, nVacated);
}

void
init_page_first_chunk(BTreeDescr *desc, Page p, LocationIndex hikeySize)
{
	BTreePageHeader *header = (BTreePageHeader *) p;

	Assert(hikeySize == MAXALIGN(hikeySize));

	header->chunksCount = 1;
	header->itemsCount = 0;

	header->hikeysEnd = MAXALIGN(sizeof(BTreePageHeader)) + hikeySize;
	if (header->hikeysEnd > BTREE_PAGE_HIKEYS_END(desc, p))
		header->dataSize = header->hikeysEnd;
	else
		header->dataSize = BTREE_PAGE_HIKEYS_END(desc, p);

	header->chunkDesc[0].hikeyShortLocation = LOCATION_GET_SHORT(MAXALIGN(sizeof(BTreePageHeader)));
	header->chunkDesc[0].shortLocation = LOCATION_GET_SHORT(header->dataSize);
	header->chunkDesc[0].offset = 0;
	header->chunkDesc[0].hikeyFlags = 0;
}

void
page_chunk_fill_locator(Page p, OffsetNumber chunkOffset,
						BTreePageItemLocator *locator)
{
	BTreePageHeader *header = (BTreePageHeader *) p;

	if (chunkOffset + 1 < header->chunksCount)
	{
		locator->chunkItemsCount = header->chunkDesc[chunkOffset + 1].offset -
			header->chunkDesc[chunkOffset].offset;
		locator->chunkSize = SHORT_GET_LOCATION(header->chunkDesc[chunkOffset + 1].shortLocation) -
			SHORT_GET_LOCATION(header->chunkDesc[chunkOffset].shortLocation);
	}
	else
	{
		locator->chunkItemsCount = header->itemsCount -
			header->chunkDesc[chunkOffset].offset;
		locator->chunkSize = header->dataSize -
			SHORT_GET_LOCATION(header->chunkDesc[chunkOffset].shortLocation);
	}
	locator->chunkOffset = chunkOffset;
	locator->itemOffset = 0;
	locator->chunk = (BTreePageChunk *) (p + SHORT_GET_LOCATION(header->chunkDesc[chunkOffset].shortLocation));
}

void
page_item_fill_locator(Page p, OffsetNumber itemOffset,
					   BTreePageItemLocator *locator)
{
	BTreePageHeader *header = (BTreePageHeader *) p;
	OffsetNumber chunkOffset;

	chunkOffset = 0;
	while (chunkOffset < header->chunksCount - 1 &&
		   itemOffset >= header->chunkDesc[chunkOffset + 1].offset)
		chunkOffset++;

	page_chunk_fill_locator(p, chunkOffset, locator);
	locator->itemOffset = itemOffset - header->chunkDesc[chunkOffset].offset;
}

void
page_item_fill_locator_backwards(Page p, OffsetNumber itemOffset,
								 BTreePageItemLocator *locator)
{
	BTreePageHeader *header = (BTreePageHeader *) p;
	OffsetNumber chunkOffset;

	chunkOffset = header->chunksCount - 1;
	while (itemOffset < header->chunkDesc[chunkOffset].offset)
	{
		Assert(chunkOffset > 0);
		chunkOffset--;
	}

	page_chunk_fill_locator(p, chunkOffset, locator);
	locator->itemOffset = itemOffset - header->chunkDesc[chunkOffset].offset;
}

/*
 * Locate the next page item.
 */
bool
page_locator_next_chunk(Page p, BTreePageItemLocator *locator)
{
	while (locator->itemOffset >= locator->chunkItemsCount)
	{
		BTreePageHeader *header = (BTreePageHeader *) p;

		if (locator->chunkOffset + 1 < header->chunksCount)
		{
			page_chunk_fill_locator(p, locator->chunkOffset + 1, locator);
		}
		else
		{
			return false;
		}
	}
	return true;
}

/*
 * Locate the next page item.
 */
bool
page_locator_prev_chunk(Page p, BTreePageItemLocator *locator)
{
	do
	{
		if (locator->chunkOffset > 0)
		{
			page_chunk_fill_locator(p, locator->chunkOffset - 1, locator);
		}
		else
		{
			locator->chunk = NULL;
			return false;
		}
	}
	while (locator->chunkItemsCount == 0);
	locator->itemOffset = locator->chunkItemsCount - 1;
	return true;
}

/*
 * Insert a new item of given size at the given location.
 */
void
page_locator_insert_item(Page p, BTreePageItemLocator *locator,
						 LocationIndex itemsize)
{
	int			itemsShift,
				dataShift;
	Pointer		firstItemPtr,
				itemPtr,
				endPtr;
	BTreePageHeader *header = (BTreePageHeader *) p;
	OffsetNumber i;

	Assert(itemsize == MAXALIGN(itemsize));

	/* Calculate the change of (maxaligned) item array size */
	itemsShift = MAXALIGN(sizeof(LocationIndex) * (locator->chunkItemsCount + 1)) -
		MAXALIGN(sizeof(LocationIndex) * locator->chunkItemsCount);

	/* Calculate the shift of the data after new item inserted */
	dataShift = itemsShift + itemsize;

	firstItemPtr = (Pointer) locator->chunk +
		MAXALIGN(sizeof(LocationIndex) * locator->chunkItemsCount);

	if (locator->itemOffset < locator->chunkItemsCount)
	{
		itemPtr = (Pointer) locator->chunk +
			ITEM_GET_OFFSET(locator->chunk->items[locator->itemOffset]);
	}
	else
	{
		Assert(locator->itemOffset == locator->chunkItemsCount);
		itemPtr = (Pointer) locator->chunk + locator->chunkSize;
	}
	endPtr = (Pointer) p + header->dataSize;

	/* Data should still fit to the page */
	Assert(endPtr + dataShift <= (Pointer) p + ORIOLEDB_BLCKSZ);

	/* Shift the data after insert location */
	Assert(itemPtr <= endPtr);
	memmove(itemPtr + dataShift, itemPtr, endPtr - itemPtr);

	/* Adjust chunks parameters */
	for (i = locator->chunkOffset + 1; i < header->chunksCount; i++)
	{
		header->chunkDesc[i].shortLocation += LOCATION_GET_SHORT(dataShift);
		header->chunkDesc[i].offset++;
	}
	header->itemsCount++;
	header->dataSize += dataShift;

	if (itemsShift != 0)
	{
		/*
		 * If items array size is changed, then we have to also move the items
		 * before insert location and adjust those locations in the items
		 * array.
		 */
		memmove(firstItemPtr + itemsShift, firstItemPtr, itemPtr - firstItemPtr);
		for (i = 0; i < locator->itemOffset; i++)
			locator->chunk->items[i] += itemsShift;
	}

	/* Add new element to the items array  */
	for (i = locator->chunkItemsCount; i > locator->itemOffset; i--)
		locator->chunk->items[i] = locator->chunk->items[i - 1] + dataShift;
	locator->chunk->items[locator->itemOffset] = (Pointer) itemPtr - (Pointer) locator->chunk + itemsShift;

	/* Adjust the locator */
	locator->chunkItemsCount++;
	locator->chunkSize += dataShift;
}

bool
page_locator_fits_new_item(Page p, BTreePageItemLocator *locator,
						   LocationIndex itemsize)
{
	LocationIndex sizeDiff;

	sizeDiff = MAXALIGN(sizeof(LocationIndex) * (locator->chunkItemsCount + 1)) -
		MAXALIGN(sizeof(LocationIndex) * locator->chunkItemsCount);

	sizeDiff += MAXALIGN(itemsize);

	return BTREE_PAGE_FREE_SPACE(p) >= sizeDiff;
}

/*
 * Get size of the item at given location.
 */
LocationIndex
page_locator_get_item_size(Page p, BTreePageItemLocator *locator)
{
	LocationIndex itemOffset,
				nextItemOffset;
	BTreePageHeader *header = (BTreePageHeader *) p;

	/* Calculate offset form the beginning of the chunk */
	itemOffset = ITEM_GET_OFFSET(locator->chunk->items[locator->itemOffset]);
	if (locator->itemOffset + 1 < locator->chunkItemsCount)
	{
		/* Next item is in the same chunk */
		nextItemOffset = ITEM_GET_OFFSET(locator->chunk->items[locator->itemOffset + 1]);
	}
	else
	{
		/*
		 * Next item is in the next chunk. Recalculate offsets from the
		 * beginning of the page.
		 */
		itemOffset += (Pointer) locator->chunk - (Pointer) p;
		if (locator->chunkOffset + 1 < header->chunksCount)
			nextItemOffset = SHORT_GET_LOCATION(header->chunkDesc[locator->chunkOffset + 1].shortLocation);
		else
			nextItemOffset = header->dataSize;
	}
	return (nextItemOffset - itemOffset);
}

/*
 * Resizes page item under given locator.
 */
void
page_locator_resize_item(Page p, BTreePageItemLocator *locator,
						 LocationIndex newsize)
{
	int			dataShift;
	Pointer		nextItemPtr,
				endPtr;
	BTreePageHeader *header = (BTreePageHeader *) p;
	OffsetNumber i;

	/* Calculate data shift */
	Assert(newsize == MAXALIGN(newsize));
	dataShift = newsize - page_locator_get_item_size(p, locator);
	Assert(dataShift == MAXALIGN(dataShift));

	if (dataShift == 0)
		return;

	Assert(locator->itemOffset < locator->chunkSize);
	if (locator->itemOffset + 1 < locator->chunkItemsCount)
		nextItemPtr = (Pointer) locator->chunk +
			ITEM_GET_OFFSET(locator->chunk->items[locator->itemOffset + 1]);
	else
		nextItemPtr = (Pointer) locator->chunk + locator->chunkSize;
	endPtr = (Pointer) p + header->dataSize;

	Assert(endPtr + dataShift <= (Pointer) p + ORIOLEDB_BLCKSZ);

	/* Shift the data after the item */
	memmove(nextItemPtr + dataShift, nextItemPtr, endPtr - nextItemPtr);

	/* Adjust chunk positions */
	for (i = locator->chunkOffset + 1; i < header->chunksCount; i++)
		header->chunkDesc[i].shortLocation += LOCATION_GET_SHORT(dataShift);
	header->dataSize += dataShift;

	/* Adjust the items array */
	for (i = locator->itemOffset + 1; i < locator->chunkItemsCount; i++)
		locator->chunk->items[i] += dataShift;

	/* Adjust the locator */
	locator->chunkSize += dataShift;
}

/*
 * Merge two chunks into one.
 */
static void
page_merge_chunks(Page p, OffsetNumber index)
{
	LocationIndex tmpItems[BTREE_PAGE_MAX_CHUNK_ITEMS],
				hikeyShift,
				hikeyShift2,
				shift1,
				shift2;
	OffsetNumber i,
				count1,
				count2;
	BTreePageHeader *header = (BTreePageHeader *) p;
	BTreePageItemLocator loc1,
				loc2;
	Pointer		chunk1DataPtr,
				chunk1EndPtr,
				chunk2DataPtr,
				endPtr,
				p1_1,
				p1_2,
				p2_1,
				p2_2;
	int			len1,
				len2;

	Assert(index + 1 < header->chunksCount);

	page_chunk_fill_locator(p, index, &loc1);
	page_chunk_fill_locator(p, index + 1, &loc2);

	count1 = loc1.chunkItemsCount;
	count2 = loc2.chunkItemsCount;

	chunk1DataPtr = (Pointer) loc1.chunk +
		MAXALIGN(sizeof(LocationIndex) * count1);
	chunk1EndPtr = (Pointer) loc1.chunk + loc1.chunkSize;

	chunk2DataPtr = (Pointer) loc2.chunk +
		MAXALIGN(sizeof(LocationIndex) * count2);
	endPtr = (Pointer) p + header->dataSize;

	shift1 = MAXALIGN(sizeof(LocationIndex) * (count1 + count2)) -
		MAXALIGN(sizeof(LocationIndex) * count1);
	shift2 = MAXALIGN(sizeof(LocationIndex) * count1) +
		MAXALIGN(sizeof(LocationIndex) * count2) -
		MAXALIGN(sizeof(LocationIndex) * (count1 + count2));

	for (i = 0; i < count2; i++)
	{
		tmpItems[i] = loc2.chunk->items[i] -
			MAXALIGN(sizeof(LocationIndex) * count2) +
			MAXALIGN(sizeof(LocationIndex) * (count1 + count2)) +
			(chunk1EndPtr - chunk1DataPtr);
	}

	if (shift1 != 0)
	{
		for (i = 0; i < count1; i++)
			loc1.chunk->items[i] += shift1;
		memmove(chunk1DataPtr + shift1,
				chunk1DataPtr,
				chunk1EndPtr - chunk1DataPtr);
	}

	if (shift2 != 0)
	{
		memmove(chunk2DataPtr - shift2,
				chunk2DataPtr,
				endPtr - chunk2DataPtr);
		header->dataSize -= shift2;
	}

	memcpy((Pointer) loc1.chunk + sizeof(LocationIndex) * count1,
		   tmpItems,
		   sizeof(LocationIndex) * count2);

	hikeyShift = MAXALIGN(offsetof(BTreePageHeader, chunkDesc) + sizeof(BTreePageChunkDesc) * header->chunksCount) -
		MAXALIGN(offsetof(BTreePageHeader, chunkDesc) + sizeof(BTreePageChunkDesc) * (header->chunksCount - 1));
	hikeyShift2 = hikeyShift +
		SHORT_GET_LOCATION(header->chunkDesc[index + 1].hikeyShortLocation) -
		SHORT_GET_LOCATION(header->chunkDesc[index].hikeyShortLocation);

	p1_1 = (Pointer) p + SHORT_GET_LOCATION(header->chunkDesc[0].hikeyShortLocation) - hikeyShift;
	p1_2 = (Pointer) p + SHORT_GET_LOCATION(header->chunkDesc[0].hikeyShortLocation);
	len1 = SHORT_GET_LOCATION(header->chunkDesc[index].hikeyShortLocation) -
		SHORT_GET_LOCATION(header->chunkDesc[0].hikeyShortLocation);

	p2_1 = (Pointer) p + SHORT_GET_LOCATION(header->chunkDesc[index].hikeyShortLocation) - hikeyShift;
	p2_2 = (Pointer) p + SHORT_GET_LOCATION(header->chunkDesc[index + 1].hikeyShortLocation);
	len2 = header->hikeysEnd - SHORT_GET_LOCATION(header->chunkDesc[index + 1].hikeyShortLocation);

	header->chunkDesc[index].hikeyFlags = header->chunkDesc[index + 1].hikeyFlags;
	for (i = index + 2; i < header->chunksCount; i++)
	{
		header->chunkDesc[i - 1].offset = header->chunkDesc[i].offset;
		header->chunkDesc[i - 1].hikeyFlags = header->chunkDesc[i].hikeyFlags;
		header->chunkDesc[i - 1].hikeyShortLocation = header->chunkDesc[i].hikeyShortLocation - LOCATION_GET_SHORT(hikeyShift2);
		header->chunkDesc[i - 1].shortLocation = header->chunkDesc[i].shortLocation - LOCATION_GET_SHORT(shift2);
	}

	if (hikeyShift > 0)
	{
		for (i = 0; i <= index; i++)
			header->chunkDesc[i].hikeyShortLocation = header->chunkDesc[i].hikeyShortLocation - LOCATION_GET_SHORT(hikeyShift);
	}

	if (hikeyShift > 0)
		memmove(p1_1, p1_2, len1);

	memmove(p2_1, p2_2, len2);

	header->hikeysEnd -= hikeyShift2;
	header->chunksCount--;
	VALGRIND_CHECK_MEM_IS_DEFINED(p, ORIOLEDB_BLCKSZ);
}

/*
 * Deletes page item under given locator.
 */
void
page_locator_delete_item(Page p, BTreePageItemLocator *locator)
{
	int			itemsShift,
				dataShift,
				itemsize;
	Pointer		firstItemPtr,
				itemPtr,
				endPtr;
	BTreePageHeader *header = (BTreePageHeader *) p;
	OffsetNumber i;

	/* Get item size */
	itemsize = page_locator_get_item_size(p, locator);
	Assert(itemsize == MAXALIGN(itemsize));

	itemsShift = MAXALIGN(sizeof(LocationIndex) * locator->chunkItemsCount) -
		MAXALIGN(sizeof(LocationIndex) * (locator->chunkItemsCount - 1));
	dataShift = itemsShift + itemsize;

	firstItemPtr = (Pointer) locator->chunk +
		MAXALIGN(sizeof(LocationIndex) * locator->chunkItemsCount);
	Assert(locator->itemOffset < locator->chunkSize);
	itemPtr = (Pointer) locator->chunk +
		ITEM_GET_OFFSET(locator->chunk->items[locator->itemOffset]);
	endPtr = (Pointer) p + header->dataSize;

	Assert(endPtr - dataShift >= itemPtr - itemsShift);

	/*
	 * Adjust the items array.  We should do this first to prevent it been
	 * overridden by the data when it's shorten.
	 */
	for (i = locator->itemOffset; i < locator->chunkItemsCount - 1; i++)
		locator->chunk->items[i] = locator->chunk->items[i + 1] - dataShift;

	if (itemsShift != 0)
	{
		/* Shift the data before deleted item when items arrays is shorten. */
		memmove(firstItemPtr - itemsShift, firstItemPtr, itemPtr - firstItemPtr);

		/* Shift item pointers of those items */
		for (i = 0; i < locator->itemOffset; i++)
			locator->chunk->items[i] -= itemsShift;
	}

	/* Move the data after deleted item */
	memmove(itemPtr - itemsShift, itemPtr + itemsize, endPtr - itemPtr - itemsize);

	/* Adjust position of following chunks */
	for (i = locator->chunkOffset + 1; i < header->chunksCount; i++)
	{
		header->chunkDesc[i].shortLocation -= LOCATION_GET_SHORT(dataShift);
		header->chunkDesc[i].offset--;
	}
	header->itemsCount--;
	header->dataSize -= dataShift;

	/* Adjust the locator */
	locator->chunkItemsCount--;
	locator->chunkSize -= dataShift;

	if (locator->chunkItemsCount == 0)
	{
		if (locator->chunkOffset > 0)
		{
			page_merge_chunks(p, locator->chunkOffset - 1);
			page_chunk_fill_locator(p, locator->chunkOffset - 1, locator);
			locator->itemOffset = locator->chunkItemsCount;
		}
		else if (locator->chunkOffset + 1 < header->chunksCount)
		{
			page_merge_chunks(p, locator->chunkOffset);
			page_chunk_fill_locator(p, locator->chunkOffset, locator);
		}
	}
}

/*
 * Split the given page chunk into two.
 */
static void
page_split_chunk(Page p, BTreePageItemLocator *locator,
				 LocationIndex hikeysEnd, LocationIndex hikeySize)
{
	LocationIndex tmpItems[BTREE_PAGE_MAX_CHUNK_ITEMS],
				leftItemsShift,
				rightItemsShift,
				dataShift,
				chunkDescShift,
				hikeyShift;
	Pointer		firstItemPtr,
				itemPtr,
				endPtr,
				rightChunkPtr,
				firstHikeyPtr,
				hikeyPtr,
				hikeyEndPtr;
	OffsetNumber i,
				leftItemsCount,
				rightItemsCount;
	BTreePageHeader *header = (BTreePageHeader *) p;

	Assert(hikeySize == MAXALIGN(hikeySize));

	leftItemsCount = locator->itemOffset;
	rightItemsCount = locator->chunkItemsCount - locator->itemOffset;
	firstItemPtr = (Pointer) locator->chunk +
		MAXALIGN(sizeof(LocationIndex) * locator->chunkItemsCount);
	itemPtr = (Pointer) locator->chunk +
		ITEM_GET_OFFSET(locator->chunk->items[locator->itemOffset]);
	endPtr = (Pointer) p + header->dataSize;
	Assert(firstItemPtr >= p && itemPtr >= firstItemPtr && endPtr >= itemPtr);
	Assert(endPtr <= (Pointer) p + ORIOLEDB_BLCKSZ);

	/*
	 * Save positions of the items, which go to the right chunk.  We have to
	 * do this in order to make these items not overridden while data is
	 * moved. Position are counted from the beginning of the new chunk.
	 */
	leftItemsShift = MAXALIGN(sizeof(LocationIndex) * locator->chunkItemsCount) -
		MAXALIGN(sizeof(LocationIndex) * leftItemsCount);
	rightItemsShift = ITEM_GET_OFFSET(locator->chunk->items[locator->itemOffset]) -
		MAXALIGN(sizeof(LocationIndex) * rightItemsCount);
	for (i = locator->itemOffset; i < locator->chunkItemsCount; i++)
		tmpItems[i - locator->itemOffset] = locator->chunk->items[i] - rightItemsShift;

	VALGRIND_CHECK_MEM_IS_DEFINED(tmpItems, sizeof(tmpItems[0]) * rightItemsCount);

	/*
	 * Move the data items belong to the left chunk accordingly to new size of
	 * items array.
	 */
	for (i = 0; i < locator->itemOffset; i++)
		locator->chunk->items[i] -= leftItemsShift;
	memmove(firstItemPtr - leftItemsShift,
			firstItemPtr,
			itemPtr - firstItemPtr);

	VALGRIND_CHECK_MEM_IS_DEFINED(p, ORIOLEDB_BLCKSZ);

	/* Shift the data items belong to the right chunk */
	dataShift = MAXALIGN(sizeof(LocationIndex) * rightItemsCount) +
		MAXALIGN(sizeof(LocationIndex) * leftItemsCount) -
		MAXALIGN(sizeof(LocationIndex) * locator->chunkItemsCount);
	Assert(itemPtr + dataShift + (endPtr - itemPtr) <= (Pointer) p + ORIOLEDB_BLCKSZ);
	memmove(itemPtr + dataShift, itemPtr, endPtr - itemPtr);

	VALGRIND_CHECK_MEM_IS_DEFINED(p, ORIOLEDB_BLCKSZ);

	/* Place the right chunk items array */
	rightChunkPtr = itemPtr -
		MAXALIGN(sizeof(LocationIndex) * locator->chunkItemsCount) +
		MAXALIGN(sizeof(LocationIndex) * leftItemsCount);
	memcpy(rightChunkPtr, tmpItems, sizeof(LocationIndex) * rightItemsCount);

	VALGRIND_CHECK_MEM_IS_DEFINED(p, ORIOLEDB_BLCKSZ);

	/* Calculate shift of hikeys before the new hikey */
	chunkDescShift = MAXALIGN(offsetof(BTreePageHeader, chunkDesc) + sizeof(BTreePageChunkDesc) * (header->chunksCount + 1)) -
		MAXALIGN(offsetof(BTreePageHeader, chunkDesc) + sizeof(BTreePageChunkDesc) * header->chunksCount);
	/* Calculate shift of hikeys after the new hikey */
	hikeyShift = chunkDescShift + hikeySize;

	firstHikeyPtr = (Pointer) p + SHORT_GET_LOCATION(header->chunkDesc[0].hikeyShortLocation);
	hikeyPtr = (Pointer) p + SHORT_GET_LOCATION(header->chunkDesc[locator->chunkOffset].hikeyShortLocation);
	hikeyEndPtr = (Pointer) p + header->hikeysEnd;
	Assert(firstHikeyPtr >= p && hikeyPtr >= firstHikeyPtr && hikeyEndPtr >= hikeyPtr);

	/* Move hikeys */
	Assert(hikeyEndPtr + hikeyShift <= (Pointer) p + hikeysEnd);
	memmove(hikeyPtr + hikeyShift, hikeyPtr, hikeyEndPtr - hikeyPtr);
	memmove(firstHikeyPtr + chunkDescShift, firstHikeyPtr, hikeyPtr - firstHikeyPtr);

	VALGRIND_CHECK_MEM_IS_DEFINED(p, ORIOLEDB_BLCKSZ);

	/* Adjust chunk descs */
	for (i = 0; i <= locator->chunkOffset; i++)
		header->chunkDesc[i].hikeyShortLocation += LOCATION_GET_SHORT(chunkDescShift);

	for (i = header->chunksCount; i > locator->chunkOffset; i--)
	{
		header->chunkDesc[i].hikeyShortLocation = header->chunkDesc[i - 1].hikeyShortLocation + LOCATION_GET_SHORT(hikeyShift);
		header->chunkDesc[i].hikeyFlags = header->chunkDesc[i - 1].hikeyFlags;
		header->chunkDesc[i].offset = header->chunkDesc[i - 1].offset;
		header->chunkDesc[i].shortLocation = header->chunkDesc[i - 1].shortLocation + LOCATION_GET_SHORT(dataShift);
	}

	i = locator->chunkOffset + 1;
	header->chunkDesc[i].hikeyShortLocation = header->chunkDesc[i - 1].hikeyShortLocation +
		LOCATION_GET_SHORT(hikeySize);
	header->chunkDesc[i].offset = header->chunkDesc[i - 1].offset + leftItemsCount;
	header->chunkDesc[i].shortLocation = LOCATION_GET_SHORT(rightChunkPtr - (Pointer) p);
	header->chunkDesc[i].hikeyFlags = header->chunkDesc[i - 1].hikeyFlags;
	header->chunksCount++;
	header->hikeysEnd += hikeyShift;
	header->dataSize += dataShift;

	VALGRIND_CHECK_MEM_IS_DEFINED(p, ORIOLEDB_BLCKSZ);

	page_chunk_fill_locator(p, i, locator);
}

#define MAXALIGN_WASTE(s) \
	((MAXIMUM_ALIGNOF - 1) - ((s) + (MAXIMUM_ALIGNOF - 1)) % (MAXIMUM_ALIGNOF))

void
page_split_chunk_if_needed(BTreeDescr *desc, Page p, BTreePageItemLocator *locator)
{
	OffsetNumber i,
				chunkOffset;
	LocationIndex hikeysFreeSpace,
				dataFreeSpace;
	BTreePageHeader *header = (BTreePageHeader *) p;
	int			bestOffset = -1;
	float4		bestScore = 0.0f;
	LocationIndex bestHiKeySize = 0,
				bestHiKeySizeUnaligned = 0,
				hikeysEnd = BTREE_PAGE_HIKEYS_END(desc, p);
	OFixedKey	newHikey;

	VALGRIND_CHECK_MEM_IS_DEFINED(p, ORIOLEDB_BLCKSZ);
	VALGRIND_MAKE_MEM_DEFINED(p, ORIOLEDB_BLCKSZ);

	if (header->hikeysEnd >= hikeysEnd)
		return;

	chunkOffset = locator->chunkOffset;

	if ((float4) locator->chunkSize / (float4) (ORIOLEDB_BLCKSZ - hikeysEnd) <
		(float4) MAXALIGN(header->maxKeyLen) * 2.0f / (float4) (hikeysEnd - offsetof(BTreePageHeader, chunkDesc)))
		return;

	hikeysFreeSpace = hikeysEnd - header->hikeysEnd;
	hikeysFreeSpace -=
		(MAXALIGN(offsetof(BTreePageHeader, chunkDesc) + sizeof(BTreePageChunkDesc) * (header->chunksCount + 1)) -
		 MAXALIGN(offsetof(BTreePageHeader, chunkDesc) + sizeof(BTreePageChunkDesc) * header->chunksCount));
	dataFreeSpace = ORIOLEDB_BLCKSZ - header->dataSize;

	for (i = 1; i < locator->chunkItemsCount; i++)
	{
		LocationIndex hikeySize,
					hikeySizeUnaligned,
					dataSize,
					leftDataSize,
					rightDataSize;
		float4		score;

		locator->itemOffset = i;
		if (O_PAGE_IS(p, LEAF))
		{
			OTuple		tuple;

			tuple.data = BTREE_PAGE_LOCATOR_GET_ITEM(p, locator) + BTreeLeafTuphdrSize;
			tuple.formatFlags = BTREE_PAGE_GET_ITEM_FLAGS(p, locator);
			hikeySizeUnaligned = o_btree_len(desc, tuple, OTupleKeyLengthNoVersion);
			hikeySize = MAXALIGN(hikeySizeUnaligned);
		}
		else
		{
			hikeySize = BTREE_PAGE_GET_ITEM_SIZE(p, locator) -
				BTreeNonLeafTuphdrSize;
			hikeySizeUnaligned = hikeySize;
		}
		if (hikeySize > hikeysFreeSpace)
			continue;

		dataSize = MAXALIGN(i * sizeof(LocationIndex)) +
			MAXALIGN((locator->chunkItemsCount - i) * sizeof(LocationIndex)) -
			MAXALIGN(locator->chunkItemsCount * sizeof(LocationIndex));

		if (dataSize > dataFreeSpace)
			continue;

		leftDataSize = ITEM_GET_OFFSET(locator->chunk->items[locator->itemOffset]);
		rightDataSize = locator->chunkSize - leftDataSize;
		leftDataSize -= MAXALIGN(locator->chunkItemsCount * sizeof(LocationIndex));

		score = (float4) Min(leftDataSize, rightDataSize) / (float4) hikeySize;

		if (score > bestScore)
		{
			bestOffset = i;
			bestHiKeySize = hikeySize;
			bestHiKeySizeUnaligned = hikeySizeUnaligned;
			bestScore = score;
		}
	}

	if (bestOffset < 0)
		return;

	locator->itemOffset = bestOffset;
	if (O_PAGE_IS(p, LEAF))
	{
		OTuple		tuple;
		bool		allocated;

		tuple.data = BTREE_PAGE_LOCATOR_GET_ITEM(p, locator) + BTreeLeafTuphdrSize;
		tuple.formatFlags = BTREE_PAGE_GET_ITEM_FLAGS(p, locator);
		newHikey.tuple = o_btree_tuple_make_key(desc, tuple, newHikey.fixedData,
												false, &allocated);
		if (bestHiKeySize != bestHiKeySizeUnaligned)
			memset(newHikey.fixedData + bestHiKeySizeUnaligned,
				   0,
				   bestHiKeySize - bestHiKeySizeUnaligned);
		Assert(allocated == false);
		VALGRIND_CHECK_MEM_IS_DEFINED(newHikey.fixedData, bestHiKeySizeUnaligned);
		VALGRIND_CHECK_MEM_IS_DEFINED(newHikey.fixedData, bestHiKeySize);
	}
	else
	{
		OTuple		key;

		key.data = BTREE_PAGE_LOCATOR_GET_ITEM(p, locator) + BTreeNonLeafTuphdrSize;
		key.formatFlags = BTREE_PAGE_GET_ITEM_FLAGS(p, locator);
		copy_fixed_key(desc, &newHikey, key);
		VALGRIND_CHECK_MEM_IS_DEFINED(newHikey.fixedData, bestHiKeySize);
	}

	VALGRIND_CHECK_MEM_IS_DEFINED(p, ORIOLEDB_BLCKSZ);

	page_split_chunk(p, locator, hikeysEnd, bestHiKeySize);

	VALGRIND_CHECK_MEM_IS_DEFINED(p, ORIOLEDB_BLCKSZ);

	memcpy(p + SHORT_GET_LOCATION(header->chunkDesc[chunkOffset].hikeyShortLocation),
		   newHikey.fixedData,
		   bestHiKeySize);
	header->chunkDesc[chunkOffset].hikeyFlags = newHikey.tuple.formatFlags;

	VALGRIND_CHECK_MEM_IS_DEFINED(p, ORIOLEDB_BLCKSZ);
}

#ifdef NOT_USED
static void
check_page(BTreeDescr *desc, Page p)
{
	BTreePageItemLocator loc;
	OTuple		prev,
				tup;
	BTreeKeyType kind = O_PAGE_IS(p, LEAF) ? BTreeTuple : BTreeKey;
	BTreePageHeader *header = (BTreePageHeader *) p;

	O_TUPLE_SET_NULL(prev);

	BTREE_PAGE_LOCATOR_FIRST(p, &loc);

	if (!O_PAGE_IS(p, LEAF))
		BTREE_PAGE_LOCATOR_NEXT(p, &loc);

	while (BTREE_PAGE_LOCATOR_IS_VALID(p, &loc))
	{
		BTREE_PAGE_READ_TUPLE(tup, p, &loc);

		if (!O_TUPLE_IS_NULL(prev))
		{
			Assert(o_btree_cmp(desc, &prev, kind, &tup, kind) < 0);
		}

		if (loc.chunkOffset < header->chunksCount - 1 || !O_PAGE_IS(p, RIGHTMOST))
		{
			OTuple		chunkHikey;

			chunkHikey.data = p + SHORT_GET_LOCATION(header->chunkDesc[loc.chunkOffset].hikeyShortLocation);
			chunkHikey.formatFlags = header->chunkDesc[loc.chunkOffset].hikeyFlags;
			Assert(o_btree_cmp(desc, &tup, kind, &chunkHikey, BTreeKey) < 0);
		}

		prev = tup;
		BTREE_PAGE_LOCATOR_NEXT(p, &loc);
	}
}
#endif

static LocationIndex
item_get_key_size(BTreeDescr *desc, bool leaf, BTreePageItem *item)
{
	OTuple		tuple;

	if (leaf)
	{
		tuple.data = item->data + (item->newItem ? 0 : BTreeLeafTuphdrSize);
		tuple.formatFlags = item->flags;
		return MAXALIGN(o_btree_len(desc, tuple, OTupleKeyLengthNoVersion));
	}
	else
	{
		Assert(!item->newItem);
		tuple.data = item->data + BTreeNonLeafTuphdrSize;
		tuple.formatFlags = item->flags;
		return MAXALIGN(o_btree_len(desc, tuple, OKeyLength));
	}
}

/*
 * Split the page containing the single chunk into multiple chunks.
 */
void
btree_page_reorg(BTreeDescr *desc, Page p, BTreePageItem *items,
				 OffsetNumber count, LocationIndex hikeySize, OTuple hikey,
				 BTreePageItemLocator *newLoc)
{
	int			chunksCount;
	LocationIndex totalDataSize,
				itemHeaderSize = O_PAGE_IS(p, LEAF) ? BTreeLeafTuphdrSize : BTreeNonLeafTuphdrSize;
	BTreePageChunk *chunk;
	BTreePageHeader *header = (BTreePageHeader *) p;
	Pointer		ptr,
				hikeysPtr;
	OffsetNumber chunkOffsets[BTREE_PAGE_MAX_CHUNKS + 1];
	LocationIndex itemsArray[BTREE_PAGE_MAX_CHUNK_ITEMS];
	int			i,
				j;
	LocationIndex hikeysFreeSpace,
				hikeysFreeSpaceLeft;
	LocationIndex dataFreeSpace,
				dataFreeSpaceLeft,
				hikeysEnd;
	bool		isRightmost = O_PAGE_IS(p, RIGHTMOST);
	LocationIndex chunkDataSize;
	LocationIndex maxKeyLen;

	VALGRIND_CHECK_MEM_IS_DEFINED(p, ORIOLEDB_BLCKSZ);
	VALGRIND_MAKE_MEM_DEFINED(p, ORIOLEDB_BLCKSZ);

	hikeysEnd = Max(BTREE_PAGE_HIKEYS_END(desc, p), MAXALIGN(sizeof(BTreePageHeader)) + MAXALIGN(hikeySize));

	totalDataSize = 0;
	for (i = 0; i < count; i++)
		totalDataSize += items[i].size;

	hikeysFreeSpaceLeft = hikeysFreeSpace = hikeysEnd - (MAXALIGN(sizeof(BTreePageHeader)) + MAXALIGN(hikeySize));
	dataFreeSpaceLeft = dataFreeSpace = (ORIOLEDB_BLCKSZ - hikeysEnd) - totalDataSize - MAXALIGN(sizeof(LocationIndex) * count);

	/*
	 * Calculate the chunks count to fit both chunks area and data area.
	 */
	maxKeyLen = MAXALIGN(hikeySize);

	/* Calculate chunks boundaries */
	chunkOffsets[0] = 0;
	j = 1;
	chunkDataSize = 0;
	if (count >= 1)
		chunkDataSize += items[0].size;
	if (O_PAGE_IS(p, LEAF) && count > 0)
		maxKeyLen = Max(maxKeyLen, item_get_key_size(desc, O_PAGE_IS(p, LEAF), &items[0]));

	for (i = 1; i < count; i++)
	{
		LocationIndex nextKeySize,
					hikeySizeDiff,
					dataSpaceDiff;
		float4		dataSizeRatio;

		nextKeySize = item_get_key_size(desc, O_PAGE_IS(p, LEAF), &items[i]);
		maxKeyLen = Max(maxKeyLen, nextKeySize);
		hikeySizeDiff = nextKeySize +
			(MAXALIGN(offsetof(BTreePageHeader, chunkDesc) + sizeof(BTreePageChunkDesc) * (j + 1)) -
			 MAXALIGN(offsetof(BTreePageHeader, chunkDesc) + sizeof(BTreePageChunkDesc) * j));
		dataSpaceDiff = MAXALIGN_WASTE(sizeof(LocationIndex) * (i - chunkOffsets[j - 1]));

		if (hikeySizeDiff > hikeysFreeSpaceLeft ||
			dataSpaceDiff > dataFreeSpaceLeft)
		{
			chunkDataSize += items[i].size;
			continue;
		}

		dataSizeRatio = (float4) chunkDataSize / (float4) totalDataSize;
		if (dataSizeRatio >= (float4) (nextKeySize + sizeof(BTreePageChunkDesc)) / (float4) hikeysFreeSpace &&
			dataSizeRatio >= (float4) dataSpaceDiff / (float4) dataFreeSpace)
		{
			hikeysFreeSpaceLeft -= hikeySizeDiff;
			dataFreeSpaceLeft -= dataSpaceDiff;
			chunkOffsets[j] = i;
			chunkDataSize = 0;
			j++;
		}

		chunkDataSize += items[i].size;
	}
	Assert(j <= BTREE_PAGE_MAX_CHUNKS);
	chunkOffsets[j] = count;
	chunksCount = j;

	/* Calculate chunk items */
	ptr = (Pointer) p + hikeysEnd;
	for (j = 0; j < chunksCount; j++)
	{
		OffsetNumber chunkItemsCount;
		LocationIndex itemShift;

		chunkItemsCount = chunkOffsets[j + 1] - chunkOffsets[j];
		itemShift = MAXALIGN(sizeof(LocationIndex) * chunkItemsCount);

		for (i = chunkOffsets[j]; i < chunkOffsets[j + 1]; i++)
		{
			itemsArray[i] = ITEM_SET_FLAGS(itemShift, items[i].flags);
			itemShift += items[i].size;
		}

		ptr += itemShift;
	}

	header->maxKeyLen = maxKeyLen;
	header->dataSize = ptr - (Pointer) p;
	header->chunksCount = chunksCount;

	/*
	 * Place the chunks data.  We need to do this backwards to be sure we only
	 * move the data forwards and not override.
	 */
	for (j = chunksCount - 1; j >= 0; j--)
	{
		OffsetNumber chunkItemsCount;

		chunkItemsCount = chunkOffsets[j + 1] - chunkOffsets[j];

		for (i = chunkOffsets[j + 1] - 1; i >= chunkOffsets[j]; i--)
		{
			ptr -= items[i].size;

			if (items[i].data >= p && items[i].data < p + ORIOLEDB_BLCKSZ &&
				ptr > items[i].data && !items[i].newItem)
				memmove(ptr, items[i].data, items[i].size);
		}

		ptr -= MAXALIGN(sizeof(LocationIndex) * chunkItemsCount);
	}

	/* Place chunks item arrays and fill chunk descs */
	Assert(ptr == (Pointer) p + hikeysEnd);
	hikeysPtr = (Pointer) p + MAXALIGN(offsetof(BTreePageHeader, chunkDesc) + sizeof(BTreePageChunkDesc) * chunksCount);
	for (j = 0; j < chunksCount; j++)
	{
		OffsetNumber chunkItemsCount;
		bool		fillNewLoc = false;

		chunkItemsCount = chunkOffsets[j + 1] - chunkOffsets[j];
		i = chunkOffsets[j];
		memmove(ptr, &itemsArray[i], sizeof(LocationIndex) * chunkItemsCount);
		chunk = (BTreePageChunk *) ptr;
		header->chunkDesc[j].shortLocation = LOCATION_GET_SHORT(ptr - (Pointer) p);
		header->chunkDesc[j].offset = chunkOffsets[j];
		ptr += MAXALIGN(sizeof(LocationIndex) * chunkItemsCount);

		for (i = chunkOffsets[j]; i < chunkOffsets[j + 1]; i++)
		{
			if (!items[i].newItem)
			{
				if (!(items[i].data >= p && items[i].data < p + ORIOLEDB_BLCKSZ) ||
					ptr < items[i].data)
					memmove(ptr, items[i].data, items[i].size);
			}
			else
			{
				newLoc->chunk = chunk;
				newLoc->chunkOffset = j;
				newLoc->itemOffset = i - chunkOffsets[j];
				newLoc->chunkItemsCount = chunkItemsCount;
				fillNewLoc = true;
			}
			ptr += items[i].size;
		}

		if (fillNewLoc)
			newLoc->chunkSize = ptr - (Pointer) chunk;

		if (j > 0)
		{
			OTuple		chunkHikeyTuple;
			LocationIndex chunkHikeySize;

			if (!items[chunkOffsets[j]].newItem)
			{
				chunkHikeyTuple.formatFlags = ITEM_GET_FLAGS(chunk->items[0]);
				chunkHikeyTuple.data = (Pointer) chunk + ITEM_GET_OFFSET(chunk->items[0]) + itemHeaderSize;
			}
			else
			{
				chunkHikeyTuple.formatFlags = items[chunkOffsets[j]].flags;
				chunkHikeyTuple.data = items[chunkOffsets[j]].data;
			}
			if (O_PAGE_IS(p, LEAF))
			{
				bool		shouldFree;

				chunkHikeyTuple = o_btree_tuple_make_key(desc, chunkHikeyTuple, hikeysPtr, false, &shouldFree);
				Assert(chunkHikeyTuple.data == hikeysPtr);
				Assert(!shouldFree);
			}

			chunkHikeySize = MAXALIGN(o_btree_len(desc, chunkHikeyTuple, OKeyLength));
			if (chunkHikeyTuple.data != hikeysPtr)
				memcpy(hikeysPtr, chunkHikeyTuple.data, chunkHikeySize);
			header->chunkDesc[j - 1].hikeyFlags = chunkHikeyTuple.formatFlags;
			header->chunkDesc[j - 1].hikeyShortLocation = LOCATION_GET_SHORT(hikeysPtr - (Pointer) p);
			hikeysPtr += chunkHikeySize;
			Assert((hikeysPtr - (Pointer) p) <= hikeysEnd);
		}
	}

	/* Place page hikey */
	if (!isRightmost)
	{
		memcpy(hikeysPtr, hikey.data, hikeySize);
		if (hikeySize != MAXALIGN(hikeySize))
			memset(hikeysPtr + hikeySize, 0, MAXALIGN(hikeySize) - hikeySize);
		header->chunkDesc[j - 1].hikeyFlags = hikey.formatFlags;
		header->chunkDesc[j - 1].hikeyShortLocation = LOCATION_GET_SHORT(hikeysPtr - (Pointer) p);
		hikeysPtr += MAXALIGN(hikeySize);
		Assert((hikeysPtr - (Pointer) p) <= hikeysEnd);
	}
	else
	{
		header->chunkDesc[j - 1].hikeyFlags = 0;
		header->chunkDesc[j - 1].hikeyShortLocation = LOCATION_GET_SHORT(hikeysPtr - (Pointer) p);
	}
	header->hikeysEnd = hikeysPtr - (Pointer) p;
	header->itemsCount = count;
	VALGRIND_CHECK_MEM_IS_DEFINED(p, ORIOLEDB_BLCKSZ);
}

void
split_page_by_chunks(BTreeDescr *desc, Page p)
{
	BTreePageItemLocator loc;
	BTreePageItem items[BTREE_PAGE_MAX_CHUNK_ITEMS];
	int			i = 0;
	OFixedKey	hikey;
	LocationIndex hikeySize;

	BTREE_PAGE_FOREACH_ITEMS(p, &loc)
	{
		items[i].data = BTREE_PAGE_LOCATOR_GET_ITEM(p, &loc);
		items[i].flags = BTREE_PAGE_GET_ITEM_FLAGS(p, &loc);
		items[i].size = BTREE_PAGE_GET_ITEM_SIZE(p, &loc);
		items[i].newItem = false;
		i++;
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

	btree_page_reorg(desc, p, items, i, hikeySize, hikey.tuple, NULL);
}

bool
page_locator_find_real_item(Page p, PartialPageState *partial,
							BTreePageItemLocator *locator)
{
	BTreePageHeader *header = (BTreePageHeader *) p;
	OffsetNumber offset;

	while (locator->itemOffset >= locator->chunkItemsCount)
	{
		if (locator->chunkOffset >= header->chunksCount - 1)
			return true;

		offset = locator->itemOffset - locator->chunkItemsCount;
		if (partial)
		{
			if (!partial_load_chunk(partial, p, locator->chunkOffset + 1))
				return false;
		}
		page_chunk_fill_locator(p, locator->chunkOffset + 1, locator);
		locator->itemOffset = offset;
	}
	return true;
}

OffsetNumber
page_locator_get_offset(Page p, BTreePageItemLocator *locator)
{
	BTreePageHeader *header = (BTreePageHeader *) p;

	return header->chunkDesc[locator->chunkOffset].offset + locator->itemOffset;
}
