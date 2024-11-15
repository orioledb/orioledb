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

const BTreeChunkOps BTreeHiKeyChunkOps;

/*
 * Implementation of hikey chunks.
 */

static inline uint8
htc_get_item_flags(BTreeHiKeyChunkDesc *chunk, OffsetNumber itemOffset)
{
	Assert(itemOffset < ((BTreeChunkDesc *) chunk)->chunkItemsCount);

	return ITEM_GET_FLAGS(chunk->chunkItems[itemOffset]);
}

static inline void
htc_set_item_flags(BTreeHiKeyChunkDesc *chunk, OffsetNumber itemOffset,
				   uint8 flags)
{
	Assert(itemOffset < ((BTreeChunkDesc *) chunk)->chunkItemsCount);

	chunk->chunkItems[itemOffset] =
		ITEM_SET_FLAGS(chunk->chunkItems[itemOffset], flags);
}

static inline Pointer
htc_get_item(BTreeHiKeyChunkDesc *chunk, OffsetNumber itemOffset)
{
	Assert(itemOffset < ((BTreeChunkDesc *) chunk)->chunkItemsCount);

	return ((BTreeChunkDesc *) chunk)->chunkData +
		ITEM_GET_OFFSET(chunk->chunkItems[itemOffset]);
}

/*
 * Get size of the item as a substraction of offset of the next item and the
 * target.
 */
static inline uint16
htc_get_item_size(BTreeHiKeyChunkDesc *hikeyChunk, OffsetNumber itemOffset)
{
	BTreeChunkDesc *chunk = (BTreeChunkDesc *) hikeyChunk;
	BTreeChunkItem itemLocation;

	Assert(chunk->chunkItemsCount > itemOffset);

	/* Calculate offset form the beginning of the chunk */
	itemLocation = ITEM_GET_OFFSET(hikeyChunk->chunkItems[itemOffset]);

	if (itemOffset + 1 < chunk->chunkItemsCount)
		return ITEM_GET_OFFSET(hikeyChunk->chunkItems[itemOffset + 1]) - itemLocation;
	else
		return ((BTreeChunkDesc *) hikeyChunk)->chunkDataSize - itemLocation;
}

/*
 * Allocate space for the new item in the chunk.  A caller is responsible to
 * copy the item afterwards.
 */
static void
htc_allocate_item(BTreeChunkDesc *chunk, OffsetNumber itemOffset,
				  uint16 itemSize)
{
	BTreeHiKeyChunkDesc *hikeyChunk = (BTreeHiKeyChunkDesc *) chunk;
	LocationIndex dataShift;
	Pointer		itemPtr,
				endPtr;

	Assert(itemSize == MAXALIGN(itemSize));
	Assert(itemOffset < BTREE_PAGE_MAX_CHUNKS);

	/* Calculate the shift of the data after new item is inserted */
	dataShift = itemSize;

	if (itemOffset < chunk->chunkItemsCount)
		itemPtr = htc_get_item(hikeyChunk, itemOffset);
	else
	{
		Assert(itemOffset == chunk->chunkItemsCount);

		itemPtr = chunk->chunkData + chunk->chunkDataSize;
	}

	endPtr = chunk->chunkData + chunk->chunkDataSize;

	/* Data should still fit to the page */
	Assert(chunk->chunkDataSize + dataShift <= ORIOLEDB_BLCKSZ);

	/* Shift the data after insert location */
	Assert(itemPtr <= endPtr);

	memmove(itemPtr + dataShift, itemPtr, endPtr - itemPtr);

	chunk->chunkItemsCount++;
	chunk->chunkDataSize += dataShift;

	/* Add the new item to the items array  */
	for (int i = chunk->chunkItemsCount - 1; i > itemOffset; i--)
		hikeyChunk->chunkItems[i] = hikeyChunk->chunkItems[i - 1] + dataShift;
	hikeyChunk->chunkItems[itemOffset] = itemPtr - chunk->chunkData;
}

/*
 * Resize the chunk to fit the new item size.  A caller is responsible to copy
 * the new item.
 */
static void
htc_resize_item(BTreeChunkDesc *chunk, OffsetNumber itemOffset,
				uint16 newItemSize)
{
	BTreeHiKeyChunkDesc *hikeyChunk = (BTreeHiKeyChunkDesc *) chunk;
	int32		dataShift;
	Pointer		nextItemPtr,
				endPtr;

	Assert(newItemSize == MAXALIGN(newItemSize));

	/* Calculate the shift of the data after the item resize */
	dataShift = newItemSize - htc_get_item_size(hikeyChunk, itemOffset);
	Assert(dataShift == MAXALIGN(dataShift));

	/* We don't move items to fill the gap if the new tuple is smaller */
	if (dataShift <= 0)
		return;

	Assert(itemOffset < chunk->chunkItemsCount);
	if (itemOffset + 1 < chunk->chunkItemsCount)
		nextItemPtr = htc_get_item(hikeyChunk, itemOffset + 1);
	else
		nextItemPtr = chunk->chunkData + chunk->chunkDataSize;

	endPtr = chunk->chunkData + chunk->chunkDataSize;

	/* Data should still fit to the page */
	Assert(chunk->chunkDataSize + dataShift <= ORIOLEDB_BLCKSZ);

	/* Shift the data after the item */
	memmove(nextItemPtr + dataShift, nextItemPtr, endPtr - nextItemPtr);

	/* Adjust the items array */
	for (OffsetNumber i = itemOffset + 1; i < chunk->chunkItemsCount; i++)
		hikeyChunk->chunkItems[i] += dataShift;

	chunk->chunkDataSize += dataShift;
}

/*
 * Delete the item from the chunk by the provided offset.
 */
static void
htc_delete_item(BTreeChunkDesc *chunk, OffsetNumber itemOffset)
{
	BTreeHiKeyChunkDesc *hikeyChunk = (BTreeHiKeyChunkDesc *) chunk;
	uint16		itemSize;
	LocationIndex dataShift;
	Pointer		itemPtr,
				endPtr;

	itemSize = htc_get_item_size(hikeyChunk, itemOffset);
	Assert(itemSize == MAXALIGN(itemSize));

	Assert(chunk->chunkItemsCount > 0);

	/* Calculate the shift of the data after the item is deleted */
	dataShift = itemSize;

	Assert(itemOffset < chunk->chunkItemsCount);
	itemPtr = htc_get_item(hikeyChunk, itemOffset);

	endPtr = chunk->chunkData + chunk->chunkDataSize;
	Assert(endPtr - dataShift >= itemPtr);

	/*
	 * Adjust the items array.
	 */
	for (OffsetNumber i = itemOffset; i < chunk->chunkItemsCount - 1; i++)
		hikeyChunk->chunkItems[i] = hikeyChunk->chunkItems[i + 1] - dataShift;

	/* Move the data after deleted item */
	memmove(itemPtr, itemPtr + itemSize, endPtr - itemPtr - itemSize);

	chunk->chunkItemsCount--;
	chunk->chunkDataSize -= dataShift;
}

/*
 * Initialize hikey tuples chunk and copy the items array.
 */
static void
htc_init(BTreeChunkDesc *chunk, Page page, OffsetNumber chunkOffset)
{
	BTreePageHeader *header = (BTreePageHeader *) page;
	BTreeHiKeyChunkDesc *hikeyChunk = (BTreeHiKeyChunkDesc *) chunk;
	LocationIndex chunkLocation =
		MAXALIGN(offsetof(BTreePageHeader, chunkDesc) +
				 (sizeof(BTreePageChunkDesc) * header->chunksCount));

	Assert(chunkOffset == 0);

	chunk->chunkData = (Pointer) page + chunkLocation;
	chunk->chunkDataSize = header->hikeysEnd - chunkLocation;

	chunk->chunkItemsCount = header->chunksCount;

	for (OffsetNumber itemOffset = 0; itemOffset < chunk->chunkItemsCount;
		 itemOffset++)
	{
		hikeyChunk->chunkItems[itemOffset] =
			SHORT_GET_LOCATION(header->chunkDesc[itemOffset].hikeyShortLocation) -
			chunkLocation;
		htc_set_item_flags(hikeyChunk, itemOffset, header->chunkDesc[itemOffset].hikeyFlags);
	}
}

static void
htc_release(BTreeChunkDesc *chunk)
{
}

static inline uint16
htc_get_tuple_size(BTreeDescr *treeDesc, OTuple tuple)
{
	return o_btree_len(treeDesc, tuple, OKeyLength);
}

/*
 * Estimate size shift of the operation over the hikey tuple chunk.
 */
static int32
htc_estimate_change(BTreeChunkDesc *chunk, OffsetNumber itemOffset,
					BTreeChunkOperationType operation, OTuple tuple)
{
	BTreeHiKeyChunkDesc *hikeyChunk = (BTreeHiKeyChunkDesc *) chunk;
	int32		sizeNeeded = 0;

	if (operation == BTreeChunkOperationInsert)
		sizeNeeded = MAXALIGN(chunk->ops->get_tuple_size(chunk->treeDesc, tuple));
	else if (operation == BTreeChunkOperationUpdate)
	{
		sizeNeeded = MAXALIGN(chunk->ops->get_tuple_size(chunk->treeDesc, tuple)) -
			htc_get_item_size(hikeyChunk, itemOffset);

		/* We don't move items to fill the gap if the new tuple is smaller */
		if (sizeNeeded < 0)
			sizeNeeded = 0;
	}
	else if (operation == BTreeChunkOperationDelete)
	{
		sizeNeeded = (-1) * htc_get_item_size(hikeyChunk, itemOffset);
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
	BTreeHiKeyChunkDesc *hikeyChunk = (BTreeHiKeyChunkDesc *) chunk;
	Pointer		itemPtr;
	uint16		tupleSize;

	Assert(tupleHeader == NULL);
	Assert(itemOffset < BTREE_PAGE_MAX_CHUNKS);

	if (operation == BTreeChunkOperationInsert ||
		operation == BTreeChunkOperationUpdate)
		tupleSize = chunk->ops->get_tuple_size(chunk->treeDesc, tuple);

	if (operation == BTreeChunkOperationInsert)
	{
		/* Allocate space for the new item, move other items if necessary */
		htc_allocate_item(chunk, itemOffset, MAXALIGN(tupleSize));

		/* Copy the new tuple */
		itemPtr = htc_get_item(hikeyChunk, itemOffset);

		memcpy(itemPtr, tuple.data, tupleSize);

		htc_set_item_flags(hikeyChunk, itemOffset, tuple.formatFlags);
	}
	else if (operation == BTreeChunkOperationUpdate)
	{
		htc_resize_item(chunk, itemOffset, MAXALIGN(tupleSize));

		/* Copy the new tuple */
		itemPtr = htc_get_item(hikeyChunk, itemOffset);

		memcpy(itemPtr, tuple.data, tupleSize);

		htc_set_item_flags(hikeyChunk, itemOffset, tuple.formatFlags);
	}
	else if (operation == BTreeChunkOperationDelete)
	{
		htc_delete_item(chunk, itemOffset);
	}
	else
		elog(ERROR, "invalid BTreeChunkOperationType: %d", operation);
}

/*
 * Compare the given key to a tuple at the given offset.  Partially load the
 * chunk from the page if necessary.
 */
static bool
htc_cmp(BTreeChunkDesc *chunk, PartialPageState *partial, Page page,
		OffsetNumber itemOffset, void *key, BTreeKeyType keyType, int *result)
{
	OTuple		tuple2 = {0};
	bool		needsFree = false;
	OBTreeKeyCmp cmpFunc = chunk->treeDesc->ops->cmp;

	Assert(itemOffset < chunk->chunkItemsCount);

	chunk->ops->read_tuple(chunk, NULL, NULL, itemOffset,
						   NULL, &tuple2, &needsFree);

	Assert(!needsFree);

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
	OffsetNumber mid,
				low,
				high;
	int			targetCmpVal,
				result;
	bool		nextkey;
	OBTreeKeyCmp cmpFunc = chunk->treeDesc->ops->cmp;

	Assert(chunk->chunkItemsCount > 0);

	low = 0;
	high = chunk->chunkItemsCount - 1;
	nextkey = (keyType != BTreeKeyPageHiKey);

	if (high < low)
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
		bool		needsFree;

		mid = low + ((high - low) / 2);

		Assert(mid < chunk->chunkItemsCount - 1);

		chunk->ops->read_tuple(chunk, NULL, NULL, mid,
							   NULL, &midTuple, &needsFree);

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
			   bool *needsFree)
{
	BTreeHiKeyChunkDesc *hikeyChunk = (BTreeHiKeyChunkDesc *) chunk;

	Assert(itemOffset < chunk->chunkItemsCount);
	Assert(tupleHeader == NULL);

	tuple->data = htc_get_item(hikeyChunk, itemOffset);
	tuple->formatFlags = htc_get_item_flags(hikeyChunk, itemOffset);

	/* Set always to false in the current implementation */
	*needsFree = false;

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
		MAXALIGN(chunkBuilder->ops->get_tuple_size(chunkBuilder->treeDesc, tuple));
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
	BTreeHiKeyChunkDesc *hikeyChunk = (BTreeHiKeyChunkDesc *) chunk;
	uint16		dataShift = 0;
	Pointer		ptr;

	chunk->treeDesc = chunkBuilder->treeDesc;
	chunk->chunkItemsCount = chunkBuilder->chunkItemsCount;

	for (int i = 0; i < chunkBuilder->chunkItemsCount; i++)
		dataShift += chunkBuilder->chunkItems[i].size;

	chunk->chunkDataSize = dataShift;
	ptr = chunk->chunkData + chunk->chunkDataSize;

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
	for (OffsetNumber i = 0; i < chunk->chunkItemsCount; i++)
	{
		hikeyChunk->chunkItems[i] = dataShift;
		htc_set_item_flags(hikeyChunk, i, chunkBuilder->chunkItems[i].flags);

		dataShift += chunkBuilder->chunkItems[i].size;
	}
}

const BTreeChunkOps BTreeHiKeyChunkOps = {
	.chunkDescSize = sizeof(BTreeHiKeyChunkDesc),
	.itemHeaderSize = 0,
	.itemKeyType = BTreeKeyNonLeafKey,
	/* Main functions */
	.init = htc_init,
	.release = htc_release,
	.estimate_change = htc_estimate_change,
	.perform_change = htc_perform_change,
	.compact = NULL,
	.cmp = htc_cmp,
	.search = htc_search,
	.read_tuple = htc_read_tuple,
	/* Builder functions */
	.builder_init = htc_builder_init,
	.builder_estimate = htc_builder_estimate,
	.builder_add = htc_builder_add,
	.builder_finish = htc_builder_finish,
	/* Utility functions */
	.get_tuple_size = htc_get_tuple_size,
};
