/*-------------------------------------------------------------------------
 *
 * hikey_chunk.c
 *		OrioleDB implementation of chunk API over hikey chunks.
 *
 * Copyright (c) 2025, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/btree/hikey_chunk.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "orioledb.h"

#include "btree/chunk_ops.h"

#include "utils/memdebug.h"

const BTreeChunkOps BTreeHiKeyChunkOps;

/*
 * Implementation of hikey chunks.
 */

static inline uint16
htc_get_chunk_items_count(BTreeChunkDesc *chunk)
{
	return ((BTreePageHeader *) chunk->page)->chunksCount;
}

static inline BTreePageChunkDesc *
htc_get_chunk_items(BTreeChunkDesc *chunk)
{
	return ((BTreePageHeader *) chunk->page)->chunkDesc;
}

static inline Pointer
htc_get_chunk_start(BTreeChunkDesc *chunk)
{
	BTreePageHeader *header = (BTreePageHeader *) chunk->page;
	LocationIndex chunkLocation =
		MAXALIGN(offsetof(BTreePageHeader, chunkDesc) +
				 (sizeof(BTreePageChunkDesc) * header->chunksCount));

	return (Pointer) chunk->page + chunkLocation;
}

static inline uint16
htc_get_chunk_size(BTreeChunkDesc *chunk)
{
	BTreePageHeader *header = (BTreePageHeader *) chunk->page;
	LocationIndex chunkLocation =
		MAXALIGN(offsetof(BTreePageHeader, chunkDesc) +
				 (sizeof(BTreePageChunkDesc) * header->chunksCount));

	return header->hikeysEnd - chunkLocation;
}

static inline uint8
htc_get_item_flags(BTreeChunkDesc *chunk, OffsetNumber itemOffset)
{
	Assert(itemOffset < htc_get_chunk_items_count(chunk));

	return htc_get_chunk_items(chunk)[itemOffset].hikeyFlags;
}

static inline void
htc_set_item_flags(BTreeChunkDesc *chunk, OffsetNumber itemOffset,
				   uint8 flags)
{
	Assert(itemOffset < htc_get_chunk_items_count(chunk));

	htc_get_chunk_items(chunk)[itemOffset].hikeyFlags = flags;
}

static inline Pointer
htc_get_item(BTreeChunkDesc *chunk, OffsetNumber itemOffset)
{
	Assert(itemOffset < htc_get_chunk_items_count(chunk));

	return htc_get_chunk_start(chunk) +
		SHORT_GET_LOCATION(htc_get_chunk_items(chunk)[itemOffset].hikeyShortLocation);
}

/*
 * Get size of the item as a substraction of offset of the next item and the
 * target.
 */
static inline uint16
htc_get_item_size(BTreeChunkDesc *chunk, OffsetNumber itemOffset)
{
	LocationIndex itemLocation;
	uint16		chunkItemsCount = htc_get_chunk_items_count(chunk);
	BTreePageChunkDesc *items;

	Assert(itemOffset < chunkItemsCount);

	items = htc_get_chunk_items(chunk);

	/* Calculate offset form the beginning of the chunk */
	itemLocation = SHORT_GET_LOCATION(items[itemOffset].hikeyShortLocation);

	if (itemOffset + 1 < chunkItemsCount)
		return SHORT_GET_LOCATION(items[itemOffset + 1].hikeyShortLocation) - itemLocation;
	else
		return htc_get_chunk_size(chunk) - itemLocation;
}

/*
 * Allocate space for the new item in the chunk.  A caller is responsible to
 * copy the item afterwards.
 */
static void
htc_allocate_item(BTreeChunkDesc *chunk, OffsetNumber itemOffset,
				  uint16 itemSize)
{
	BTreePageHeader *header = (BTreePageHeader *) chunk->page;
	BTreePageChunkDesc *items = htc_get_chunk_items(chunk);
	Pointer		chunkStart = htc_get_chunk_start(chunk);
	uint16		chunkSize = htc_get_chunk_size(chunk);
	LocationIndex itemsShift,
				dataShift;
	Pointer		itemPtr,
				endPtr;

	Assert(itemSize == MAXALIGN(itemSize));
	Assert(itemOffset < BTREE_PAGE_MAX_CHUNKS);

	itemsShift = MAXALIGN(offsetof(BTreePageHeader, chunkDesc) + (sizeof(BTreePageChunkDesc) * (header->chunksCount + 1))) -
		MAXALIGN(offsetof(BTreePageHeader, chunkDesc) + (sizeof(BTreePageChunkDesc) * header->chunksCount));

	/* Calculate the shift of the data after new item is inserted */
	dataShift = itemsShift + itemSize;

	if (itemOffset < header->chunksCount)
		itemPtr = htc_get_item(chunk, itemOffset);
	else
	{
		Assert(itemOffset == header->chunksCount);

		itemPtr = chunkStart + chunkSize;
	}

	endPtr = chunkStart + chunkSize;

	/* Data should still fit to the page */
	Assert(chunkSize + dataShift <= ORIOLEDB_BLCKSZ);

	/* Shift the data after insert location */
	Assert(itemPtr <= endPtr);

	memmove(itemPtr + dataShift, itemPtr, endPtr - itemPtr);

	header->chunksCount++;
	header->hikeysEnd += dataShift;

	chunkSize += dataShift;

	if (itemsShift != 0)
	{
		/*
		 * If items array size is changed, then we have to also move the items
		 * before insert location and adjust those locations in the items
		 * array.
		 */
		memmove(chunkStart + itemsShift, chunkStart, itemPtr - chunkStart);
		for (OffsetNumber i = 0; i < itemOffset; i++)
			items[i].hikeyShortLocation += LOCATION_GET_SHORT(itemsShift);
	}

	/* Add the new item to the items array  */
	for (int i = header->chunksCount - 1; i > itemOffset; i--)
		items[i].hikeyShortLocation = items[i - 1].hikeyShortLocation +
			LOCATION_GET_SHORT(dataShift);
	items[itemOffset].hikeyShortLocation = LOCATION_GET_SHORT(itemPtr - chunk->page);
}

/*
 * Resize the chunk to fit the new item size.  A caller is responsible to copy
 * the new item.
 */
static void
htc_resize_item(BTreeChunkDesc *chunk, OffsetNumber itemOffset,
				uint16 newItemSize)
{
	BTreePageHeader *header = (BTreePageHeader *) chunk->page;
	BTreePageChunkDesc *items = htc_get_chunk_items(chunk);
	Pointer		chunkStart = htc_get_chunk_start(chunk);
	uint16		chunkSize = htc_get_chunk_size(chunk);
	int32		dataShift;
	Pointer		nextItemPtr,
				endPtr;

	Assert(newItemSize == MAXALIGN(newItemSize));

	/* Calculate the shift of the data after the item resize */
	dataShift = newItemSize - htc_get_item_size(chunk, itemOffset);
	Assert(dataShift == MAXALIGN(dataShift));

	/* We don't move items to fill the gap if the new tuple is smaller */
	if (dataShift <= 0)
		return;

	Assert(itemOffset < header->chunksCount);

	if (itemOffset + 1 < header->chunksCount)
		nextItemPtr = htc_get_item(chunk, itemOffset + 1);
	else
		nextItemPtr = chunkStart + chunkSize;

	endPtr = chunkStart + chunkSize;

	/* Data should still fit to the page */
	Assert(chunkSize + dataShift <= ORIOLEDB_BLCKSZ);

	/* Shift the data after the item */
	memmove(nextItemPtr + dataShift, nextItemPtr, endPtr - nextItemPtr);

	/* Adjust the items array */
	for (OffsetNumber i = itemOffset + 1; i < header->chunksCount; i++)
		items[i].hikeyShortLocation += LOCATION_GET_SHORT(dataShift);

	header->hikeysEnd += dataShift;
}

/*
 * Delete the item from the chunk by the provided offset.
 */
static void
htc_delete_item(BTreeChunkDesc *chunk, OffsetNumber itemOffset)
{
	BTreePageHeader *header = (BTreePageHeader *) chunk->page;
	BTreePageChunkDesc *items = htc_get_chunk_items(chunk);
	Pointer		chunkStart = htc_get_chunk_start(chunk);
	uint16		chunkSize = htc_get_chunk_size(chunk);
	uint16		itemSize;
	LocationIndex itemsShift,
				dataShift;
	Pointer		itemPtr,
				endPtr;

	itemSize = htc_get_item_size(chunk, itemOffset);
	Assert(itemSize == MAXALIGN(itemSize));

	Assert(header->chunksCount > 0 && header->chunksCount < itemOffset);

	itemsShift = MAXALIGN(offsetof(BTreePageHeader, chunkDesc) + (sizeof(BTreePageChunkDesc) * (header->chunksCount))) -
		MAXALIGN(offsetof(BTreePageHeader, chunkDesc) + (sizeof(BTreePageChunkDesc) * header->chunksCount - 1));

	/* Calculate the shift of the data after the item is deleted */
	dataShift = itemsShift + itemSize;

	itemPtr = htc_get_item(chunk, itemOffset);

	endPtr = chunkStart + chunkSize;
	Assert(endPtr - dataShift >= itemPtr - itemsShift);

	/*
	 * Adjust the items array.
	 */
	for (OffsetNumber i = itemOffset; i < header->chunksCount - 1; i++)
		items[i].hikeyShortLocation = items[i + 1].hikeyShortLocation -
			LOCATION_GET_SHORT(dataShift);

	if (itemsShift != 0)
	{
		/* Shift the data before deleted item when items arrays is shorten. */
		memmove(chunkStart - itemsShift, chunkStart, itemPtr - chunkStart);

		/* Shift item pointers of those items */
		for (OffsetNumber i = 0; i < itemOffset; i++)
			items[i].hikeyShortLocation -= LOCATION_GET_SHORT(itemsShift);
	}

	/* Move the data after deleted item */
	memmove(itemPtr, itemPtr + itemSize, endPtr - itemPtr - itemSize);

	header->chunksCount--;
	header->hikeysEnd -= dataShift;
}

/*
 * Initialize hikey tuples chunk and copy the items array.
 */
static void
htc_init(BTreeChunkDesc *chunk, Page page, OffsetNumber chunkOffset)
{
	Assert(chunkOffset == 0);

	chunk->page = page;
}

/*
 * Estimate size shift of the operation over the hikey tuple chunk.
 */
static int32
htc_estimate_change(BTreeChunkDesc *chunk, OffsetNumber itemOffset,
					BTreeChunkOperationType operation, OTuple tuple)
{
	BTreePageHeader *header = (BTreePageHeader *) chunk->page;
	int32		itemsShift = 0,
				sizeNeeded = 0;

	if (operation == BTreeChunkOperationInsert)
	{
		itemsShift = MAXALIGN(offsetof(BTreePageHeader, chunkDesc) + (sizeof(BTreePageChunkDesc) * (header->chunksCount + 1))) -
			MAXALIGN(offsetof(BTreePageHeader, chunkDesc) + (sizeof(BTreePageChunkDesc) * header->chunksCount));

		sizeNeeded = MAXALIGN(o_btree_len(chunk->treeDesc, tuple,
										  chunk->ops->itemLengthType)) +
			itemsShift;
	}
	else if (operation == BTreeChunkOperationUpdate)
	{
		sizeNeeded = MAXALIGN(o_btree_len(chunk->treeDesc, tuple,
										  chunk->ops->itemLengthType)) -
			htc_get_item_size(chunk, itemOffset);

		/* We don't move items to fill the gap if the new tuple is smaller */
		if (sizeNeeded < 0)
			sizeNeeded = 0;
	}
	else if (operation == BTreeChunkOperationDelete)
	{
		itemsShift = MAXALIGN(offsetof(BTreePageHeader, chunkDesc) + (sizeof(BTreePageChunkDesc) * (header->chunksCount))) -
			MAXALIGN(offsetof(BTreePageHeader, chunkDesc) + (sizeof(BTreePageChunkDesc) * header->chunksCount - 1));

		sizeNeeded = (-1) * (htc_get_item_size(chunk, itemOffset) + itemsShift);
	}
	else
		Assert(false);

	return sizeNeeded;
}

/*
 * Perform the operation over the hikey tuple chunk.  A caller is responsible
 * that the chunk has enough space.
 */
static void
htc_perform_change(BTreeChunkDesc *chunk, OffsetNumber itemOffset,
				   BTreeChunkOperationType operation,
				   Pointer tupleHeader, OTuple tuple)
{
	Pointer		itemPtr;
	uint16		tupleSize;

	Assert(tupleHeader == NULL);
	Assert(itemOffset < BTREE_PAGE_MAX_CHUNKS);

	if (operation == BTreeChunkOperationInsert ||
		operation == BTreeChunkOperationUpdate)
		tupleSize = o_btree_len(chunk->treeDesc, tuple,
								chunk->ops->itemLengthType);

	if (operation == BTreeChunkOperationInsert)
	{
		/* Allocate space for the new item, move other items if necessary */
		htc_allocate_item(chunk, itemOffset, MAXALIGN(tupleSize));

		/* Copy the new tuple */
		itemPtr = htc_get_item(chunk, itemOffset);

		memcpy(itemPtr, tuple.data, tupleSize);

		htc_set_item_flags(chunk, itemOffset, tuple.formatFlags);
	}
	else if (operation == BTreeChunkOperationUpdate)
	{
		htc_resize_item(chunk, itemOffset, MAXALIGN(tupleSize));

		/* Copy the new tuple */
		itemPtr = htc_get_item(chunk, itemOffset);

		memcpy(itemPtr, tuple.data, tupleSize);

		htc_set_item_flags(chunk, itemOffset, tuple.formatFlags);
	}
	else if (operation == BTreeChunkOperationDelete)
	{
		htc_delete_item(chunk, itemOffset);
	}
	else
		elog(ERROR, "invalid BTreeChunkOperationType: %d", operation);
}

/*
 * Compare the given key to a tuple at the given offset.
 */
static bool
htc_cmp(BTreeChunkDesc *chunk, PartialPageState *partial, Page page,
		OffsetNumber itemOffset, void *key, BTreeKeyType keyType, int *result)
{
	OTuple		tuple2 = {0};
	bool		isCopy = false;
	OBTreeKeyCmp cmpFunc = chunk->treeDesc->ops->cmp;

	Assert(itemOffset < htc_get_chunk_items_count(chunk));
	Assert(partial == NULL);

	chunk->ops->read_tuple(chunk, NULL, NULL, itemOffset,
						   NULL, &tuple2, &isCopy);

	Assert(!isCopy);

	*result = cmpFunc(chunk->treeDesc, key, keyType,
					  &tuple2, chunk->ops->itemKeyType);
	return true;
}

/*
 * Search for a item by the given key.
 */
static bool
htc_search(BTreeChunkDesc *chunk, PartialPageState *partial, Page page,
		   void *key, BTreeKeyType keyType, OffsetNumber *itemOffset)
{
	uint16		chunkItemsCount = htc_get_chunk_items_count(chunk);
	OffsetNumber mid,
				low,
				high;
	int			targetCmpVal,
				result;
	bool		nextkey;
	OBTreeKeyCmp cmpFunc = chunk->treeDesc->ops->cmp;

	Assert(chunkItemsCount > 0);
	Assert(partial == NULL);

	low = 0;
	high = chunkItemsCount - 1;
	nextkey = (keyType != BTreeKeyPageHiKey);

	if (unlikely(high < low))
		return low;

	targetCmpVal = nextkey ? 0 : 1; /* a target value of cmpFunc() */

	/*
	 * Don't pass BTreeHiKey to comparison function, we've set nextkey flag
	 * instead.
	 */
	if (keyType == BTreeKeyPageHiKey)
		keyType = BTreeKeyNonLeafKey;

	while (high > low)
	{
		OTuple		midTuple;
		bool		isCopy;

		mid = low + ((high - low) / 2);

		Assert(mid < chunkItemsCount - 1);

		chunk->ops->read_tuple(chunk, NULL, NULL, mid,
							   NULL, &midTuple, &isCopy);

		result = cmpFunc(chunk->treeDesc, key, keyType, &midTuple,
						 BTreeKeyNonLeafKey);

		if (result >= targetCmpVal)
			low = mid + 1;
		else
			high = mid;
	}

	*itemOffset = low;

	return true;
}

/*
 * Read a chunk item by the given offset.
 */
static bool
htc_read_tuple(BTreeChunkDesc *chunk, PartialPageState *partial, Page page,
			   OffsetNumber itemOffset, Pointer *tupleHeader, OTuple *tuple,
			   bool *isCopy)
{
	Assert(itemOffset < htc_get_chunk_items_count(chunk));
	Assert(tupleHeader == NULL);
	Assert(partial == NULL);

	tuple->data = htc_get_item(chunk, itemOffset);
	tuple->formatFlags = htc_get_item_flags(chunk, itemOffset);

	/* Set always to false in the current implementation */
	*isCopy = false;

	return true;
}

/*
 * Initialize hikey chunk builder.
 */
static void
htc_builder_init(BTreeChunkBuilder *chunkBuilder)
{
	chunkBuilder->chunkItemsCount = 0;
}

/*
 * Estimate the size shift of the result chunk after the operation.
 */
static int32
htc_builder_estimate(BTreeChunkBuilder *chunkBuilder, OTuple tuple)
{
	return chunkBuilder->ops->itemHeaderSize +
		MAXALIGN(o_btree_len(chunkBuilder->treeDesc, tuple, OKeyLength));
}

/*
 * Copy the pointer to the tuple to the chunk builder buffer.
 */
static void
htc_builder_add(BTreeChunkBuilder *chunkBuilder, BTreeChunkBuilderItem *chunkItem)
{
	Assert(chunkBuilder->chunkItemsCount < BTREE_PAGE_MAX_CHUNK_ITEMS);

	chunkBuilder->chunkItems[chunkBuilder->chunkItemsCount++] = *chunkItem;
}

/*
 * Finalizer the builder and copy tuples to the new chunk buffer.
 */
static void
htc_builder_finish(BTreeChunkBuilder *chunkBuilder, BTreeChunkDesc *chunk)
{
	BTreePageHeader *header = (BTreePageHeader *) chunk->page;
	BTreePageChunkDesc *items = htc_get_chunk_items(chunk);
	uint16		dataShift = 0;
	Pointer		ptr;

	header->chunksCount = chunkBuilder->chunkItemsCount;

	dataShift = MAXALIGN(offsetof(BTreePageHeader, chunkDesc) + (sizeof(BTreePageChunkDesc) * (header->chunksCount)));
	for (int i = 0; i < chunkBuilder->chunkItemsCount; i++)
		dataShift += chunkBuilder->chunkItems[i].size;

	header->hikeysEnd = dataShift;
	ptr = chunk->page + header->hikeysEnd;

	/*
	 * Move the tuples backwards to be sure that data is not overridden.
	 */
	for (int i = chunkBuilder->chunkItemsCount - 1; i >= 0; i--)
	{
		ptr -= chunkBuilder->chunkItems[i].size;
		memmove(ptr, chunkBuilder->chunkItems[i].data, chunkBuilder->chunkItems[i].size);
	}

	/*
	 * Calculate the chunk items offsets.
	 */
	dataShift = 0;
	for (OffsetNumber i = 0; i < header->chunksCount; i++)
	{
		items[i].hikeyShortLocation = LOCATION_GET_SHORT(dataShift);
		items[i].hikeyFlags = chunkBuilder->chunkItems[i].flags;

		dataShift += chunkBuilder->chunkItems[i].size;
	}

	VALGRIND_CHECK_MEM_IS_DEFINED(chunk->chunkData, chunk->chunkDataSize);
}

const BTreeChunkOps BTreeHiKeyChunkOps = {
	.itemHeaderSize = 0,
	.itemKeyType = BTreeKeyNonLeafKey,
	.itemLengthType = OKeyLength,
	/* Main functions */
	.init = htc_init,
	.estimate_change = htc_estimate_change,
	.perform_change = htc_perform_change,
	.compact = NULL,
	.get_available_size = NULL,
	.cmp = htc_cmp,
	.search = htc_search,
	.read_tuple = htc_read_tuple,
	/* Builder functions */
	.builder_init = htc_builder_init,
	.builder_estimate = htc_builder_estimate,
	.builder_add = htc_builder_add,
	.builder_finish = htc_builder_finish,
};
