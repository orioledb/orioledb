/*-------------------------------------------------------------------------
 *
 * tuple_chunk.c
 *		OrioleDB implementation of chunk API over tuple chunks.
 *
 * Copyright (c) 2025, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/btree/tuple_chunk.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "orioledb.h"

#include "btree/btree.h"

#include "btree/chunk_ops.h"
#include "btree/find.h"

const BTreeChunkOps LeafTupleChunkOps;
const BTreeChunkOps InternalTupleChunkOps;
const BTreeChunkOps HiKeyChunkOps;

/*
 * Implementation of leaf tuple chunks.
 */

/*
 * Get the start of the chunk and its size within the page.
 */
static inline void
ltc_get_chunk_data(Page page, OffsetNumber chunkOffset, Pointer *chunkData,
				   uint16 *chunkDataSize)
{
	BTreePageHeader *header = (BTreePageHeader *) page;
	LocationIndex chunkLocation = SHORT_GET_LOCATION(header->chunkDesc[chunkOffset].shortLocation);

	*chunkData = (Pointer) page + chunkLocation;
	if (chunkOffset + 1 < header->chunksCount)
		*chunkDataSize = SHORT_GET_LOCATION(header->chunkDesc[chunkOffset + 1].shortLocation) -
			chunkLocation;
	else
		*chunkDataSize = header->dataSize - chunkLocation;
}

static inline void
ltc_get_chunk_items(Page page, OffsetNumber chunkOffset,
					BTreeChunkItem **chunkItems, uint16 *chunkItemsCount)
{
	BTreePageHeader *header = (BTreePageHeader *) page;
	LocationIndex chunkLocation = SHORT_GET_LOCATION(header->chunkDesc[chunkOffset].shortLocation);

	*chunkItems = (BTreeChunkItem *) (page + chunkLocation);
	if (chunkOffset + 1 < header->chunksCount)
		*chunkItemsCount = header->chunkDesc[chunkOffset + 1].offset -
			header->chunkDesc[chunkOffset].offset;
	else
		*chunkItemsCount = header->itemsCount -
			header->chunkDesc[chunkOffset].offset;
}

static inline uint8
ltc_get_item_flags(BTreeTupleChunkDesc *chunk, OffsetNumber itemOffset)
{
	Assert(itemOffset < chunk->chunkItemsCount);

	return ITEM_GET_FLAGS(chunk->chunkItems->items[itemOffset]);
}

static inline void
ltc_set_item_flags(BTreeTupleChunkDesc *chunk, OffsetNumber itemOffset,
				   uint8 flags)
{
	Assert(itemOffset < chunk->chunkItemsCount);

	chunk->chunkItems->items[itemOffset] =
		ITEM_SET_FLAGS(chunk->chunkItems->items[itemOffset], flags);
}

static inline Pointer
ltc_get_item(BTreeTupleChunkDesc *chunk, OffsetNumber itemOffset)
{
	Assert(itemOffset < chunk->chunkItemsCount);

	return ((BTreeChunkDesc *) chunk)->chunkData +
		ITEM_GET_OFFSET(chunk->chunkItems->items[itemOffset]);
}

/*
 * Get size of the item as a substraction of offset of the next item and the
 * target.
 */
static inline uint16
ltc_get_item_size(BTreeTupleChunkDesc *chunk, OffsetNumber itemOffset)
{
	LocationIndex itemLocation,
				nextItemLocation;

	Assert(chunk->chunkItemsCount > itemOffset);

	/* Calculate offset form the beginning of the chunk */
	itemLocation = ITEM_GET_OFFSET(chunk->chunkItems->items[itemOffset]);

	if (itemOffset + 1 < chunk->chunkItemsCount)
	{
		nextItemLocation = ITEM_GET_OFFSET(chunk->chunkItems->items[itemOffset + 1]);
		return nextItemLocation - itemLocation;
	}
	else
		return ((BTreeChunkDesc *) chunk)->chunkDataSize - itemLocation;
}

/*
 * Allocate space for the new item in the chunk.  A caller is responsible to
 * copy the item afterwards.
 */
static void
ltc_allocate_item(BTreeChunkDesc *chunk, OffsetNumber itemOffset,
				  uint16 itemSize)
{
	BTreeTupleChunkDesc *tupleChunk = (BTreeTupleChunkDesc *) chunk;
	LocationIndex itemsShift,
				dataShift;
	Pointer		firstItemPtr,
				itemPtr,
				endPtr;

	Assert(itemSize == MAXALIGN(itemSize));

	/* Calculate the change of (maxaligned) item array size */
	itemsShift = MAXALIGN(sizeof(LocationIndex) * (tupleChunk->chunkItemsCount + 1)) -
		MAXALIGN(sizeof(LocationIndex) * tupleChunk->chunkItemsCount);

	/* Calculate the shift of the data after new item is inserted */
	dataShift = itemsShift + itemSize;
	Assert(dataShift == MAXALIGN(dataShift));

	if (tupleChunk->chunkItemsCount > 0)
		firstItemPtr = ltc_get_item(tupleChunk, 0);
	else
		firstItemPtr = chunk->chunkData +
			MAXALIGN(sizeof(LocationIndex) * tupleChunk->chunkItemsCount);

	if (itemOffset < tupleChunk->chunkItemsCount)
		itemPtr = ltc_get_item(tupleChunk, itemOffset);
	else
	{
		Assert(itemOffset == tupleChunk->chunkItemsCount);

		itemPtr = chunk->chunkData + chunk->chunkDataSize;
	}

	endPtr = chunk->chunkData + chunk->chunkDataSize;

	/* Data should still fit to the page */
	Assert(chunk->chunkDataSize + dataShift <= ORIOLEDB_BLCKSZ);

	/* Shift the data after insert location */
	Assert(itemPtr <= endPtr);

	memmove(itemPtr + dataShift, itemPtr, endPtr - itemPtr);

	tupleChunk->chunkItemsCount++;
	chunk->chunkDataSize += dataShift;

	if (itemsShift != 0)
	{
		/*
		 * If items array size is changed, then we have to also move the items
		 * before insert location and adjust those locations in the items
		 * array.
		 */
		memmove(firstItemPtr + itemsShift, firstItemPtr, itemPtr - firstItemPtr);
		for (OffsetNumber i = 0; i < itemOffset; i++)
			tupleChunk->chunkItems->items[i] += itemsShift;
	}

	/* Add the new item to the items array  */
	for (OffsetNumber i = tupleChunk->chunkItemsCount; i > itemOffset; i--)
		tupleChunk->chunkItems->items[i] = tupleChunk->chunkItems->items[i - 1] + dataShift;
	tupleChunk->chunkItems->items[itemOffset] = itemPtr - chunk->chunkData + itemsShift;
}

/*
 * Resize the chunk to fit the new item size.  A caller is responsible to copy
 * the new item.
 */
static void
ltc_resize_item(BTreeChunkDesc *chunk, OffsetNumber itemOffset,
				uint16 newItemSize)
{
	BTreeTupleChunkDesc *tupleChunk = (BTreeTupleChunkDesc *) chunk;
	int32		dataShift;
	Pointer		nextItemPtr,
				endPtr;

	Assert(newItemSize == MAXALIGN(newItemSize));

	/* Calculate the shift of the data after the item resize */
	dataShift = newItemSize - ltc_get_item_size(tupleChunk, itemOffset);
	Assert(dataShift == MAXALIGN(dataShift));

	/* We don't move items to fill the gap if the new tuple is smaller */
	if (dataShift <= 0)
		return;

	Assert(itemOffset < tupleChunk->chunkItemsCount);
	if (itemOffset + 1 < tupleChunk->chunkItemsCount)
		nextItemPtr = ltc_get_item(tupleChunk, itemOffset + 1);
	else
		nextItemPtr = chunk->chunkData + chunk->chunkDataSize;

	endPtr = chunk->chunkData + chunk->chunkDataSize;

	/* Data should still fit to the page */
	Assert(chunk->chunkDataSize + dataShift <= ORIOLEDB_BLCKSZ);

	/* Shift the data after the item */
	memmove(nextItemPtr + dataShift, nextItemPtr, endPtr - nextItemPtr);

	/* Adjust the items array */
	for (OffsetNumber i = itemOffset + 1; i < tupleChunk->chunkItemsCount; i++)
		tupleChunk->chunkItems->items[i] += dataShift;

	chunk->chunkDataSize += dataShift;
}

/*
 * Delete the item from the chunk by the provided offset.
 */
static void
ltc_delete_item(BTreeChunkDesc *chunk, OffsetNumber itemOffset)
{
	BTreeTupleChunkDesc *tupleChunk = (BTreeTupleChunkDesc *) chunk;
	uint16		itemSize;
	LocationIndex itemsShift,
				dataShift;
	Pointer		firstItemPtr,
				itemPtr,
				endPtr;

	itemSize = ltc_get_item_size(tupleChunk, itemOffset);
	Assert(itemSize == MAXALIGN(itemSize));

	Assert(tupleChunk->chunkItemsCount > 0);

	/* Calculate the change of (maxaligned) item array size */
	itemsShift = MAXALIGN(sizeof(LocationIndex) * tupleChunk->chunkItemsCount) -
		MAXALIGN(sizeof(LocationIndex) * (tupleChunk->chunkItemsCount - 1));

	/* Calculate the shift of the data after the item is deleted */
	dataShift = itemsShift + itemSize;
	Assert(dataShift == MAXALIGN(dataShift));

	firstItemPtr = ltc_get_item(tupleChunk, 0);

	Assert(itemOffset < tupleChunk->chunkItemsCount);
	itemPtr = ltc_get_item(tupleChunk, itemOffset);

	endPtr = chunk->chunkData + chunk->chunkDataSize;
	Assert(endPtr - dataShift >= itemPtr - itemsShift);

	/*
	 * Adjust the items array.  We should do this first to prevent it been
	 * overridden by the data when it's shorten.
	 */
	for (OffsetNumber i = itemOffset; i < tupleChunk->chunkItemsCount - 1; i++)
		tupleChunk->chunkItems->items[i] = tupleChunk->chunkItems->items[i + 1] - dataShift;

	if (itemsShift != 0)
	{
		/* Shift the data before deleted item when items arrays is shorten. */
		memmove(firstItemPtr - itemsShift, firstItemPtr, itemPtr - firstItemPtr);

		/* Shift item pointers of those items */
		for (OffsetNumber i = 0; i < itemOffset; i++)
			tupleChunk->chunkItems->items[i] -= itemsShift;
	}

	/* Move the data after deleted item */
	memmove(itemPtr - itemsShift, itemPtr + itemSize, endPtr - itemPtr - itemSize);

	tupleChunk->chunkItemsCount--;
	chunk->chunkDataSize -= dataShift;
}

static bool
ltc_load_partially(BTreeChunkDesc *chunk, PartialPageState *partial, Page page)
{
	BTreeTupleChunkDesc *tupleChunk = (BTreeTupleChunkDesc *) chunk;
	Page		sourcePage = partial->src;
	uint32		pageState,
				sourceState;
	BTreePageHeader *header = (BTreePageHeader *) page;
	LocationIndex chunkLocation =
		SHORT_GET_LOCATION(header->chunkDesc[tupleChunk->chunkOffset].shortLocation);
	Pointer		chunkData;
	uint16		chunkDataSize;
	BTreeChunkItem *chunkItems;
	uint16		chunkItemsCount;

	Assert(partial != NULL);

	if (!partial->isPartial || partial->chunkIsLoaded[tupleChunk->chunkOffset])
		return true;

	ltc_get_chunk_data(page, tupleChunk->chunkOffset,
					   &chunkData, &chunkDataSize);
	ltc_get_chunk_items(page, tupleChunk->chunkOffset,
						&chunkItems, &chunkItemsCount);

	chunk->ops->init(chunk, chunkData, chunkDataSize,
					 chunkItems, chunkItemsCount, tupleChunk->chunkOffset);

	memcpy(chunk->chunkData, (Pointer) sourcePage + chunkLocation, chunk->chunkDataSize);

	pg_read_barrier();

	pageState = pg_atomic_read_u32(&(O_PAGE_HEADER(page)->state));
	sourceState = pg_atomic_read_u32(&(O_PAGE_HEADER(sourcePage)->state));
	if ((pageState & PAGE_STATE_CHANGE_COUNT_MASK) != (sourceState & PAGE_STATE_CHANGE_COUNT_MASK) ||
		O_PAGE_STATE_READ_IS_BLOCKED(sourceState))
		return false;

	if (O_PAGE_GET_CHANGE_COUNT(page) != O_PAGE_GET_CHANGE_COUNT(sourcePage))
		return false;

	partial->chunkIsLoaded[tupleChunk->chunkOffset] = true;

	return true;
}

/*
 * Initialize leaf tuple chunk.  Directly use passed pointers to the chunk and
 * the chunk items array as is.
 */
static void
ltc_init(BTreeChunkDesc *chunk, Pointer chunkData,
		 uint16 chunkDataSize, BTreeChunkItem *chunkItems,
		 uint16 chunkItemsCount, OffsetNumber chunkOffset)
{
	BTreeTupleChunkDesc *tupleChunk = (BTreeTupleChunkDesc *) chunk;

	chunk->chunkData = chunkData;
	chunk->chunkDataSize = chunkDataSize;

	tupleChunk->chunkOffset = chunkOffset;
	tupleChunk->chunkItems = (BTreeChunkItem *) chunk->chunkData;
	tupleChunk->chunkItemsCount = chunkItemsCount;
}

/*
 * Estimate size shift of the operation over the leaf tuple chunk.
 */
static int32
ltc_estimate_change(BTreeChunkDesc *chunk, OffsetNumber itemOffset,
					BTreeChunkOperationType operation, OTuple tuple)
{
	BTreeTupleChunkDesc *tupleChunk = (BTreeTupleChunkDesc *) chunk;
	int32		itemsShift = 0,
				sizeNeeded = 0;

	if (operation == BTreeChunkOperationInsert)
	{
		/* Calculate the change of (maxaligned) item array size */
		itemsShift = MAXALIGN(sizeof(LocationIndex) * (tupleChunk->chunkItemsCount + 1)) -
			MAXALIGN(sizeof(LocationIndex) * tupleChunk->chunkItemsCount);

		sizeNeeded = chunk->ops->itemHeaderSize +
			MAXALIGN(chunk->ops->get_tuple_size(chunk->treeDesc, tuple)) +
			itemsShift;
	}
	else if (operation == BTreeChunkOperationUpdate)
	{
		sizeNeeded = (chunk->ops->itemHeaderSize +
					  MAXALIGN(chunk->ops->get_tuple_size(chunk->treeDesc, tuple))) -
			ltc_get_item_size(tupleChunk, itemOffset);

		/* We don't move items to fill the gap if the new tuple is smaller */
		if (sizeNeeded < 0)
			sizeNeeded = 0;
	}
	else if (operation == BTreeChunkOperationDelete)
	{
		/* Calculate the change of (maxaligned) item array size */
		itemsShift = MAXALIGN(sizeof(LocationIndex) * tupleChunk->chunkItemsCount) -
			MAXALIGN(sizeof(LocationIndex) * (tupleChunk->chunkItemsCount - 1));

		sizeNeeded = (-1) * (ltc_get_item_size(tupleChunk, itemOffset) +
							 itemsShift);
	}
	else
		Assert(false);

	return sizeNeeded;
}

/*
 * Perform the operation over the leaf tuple chunk.  A caller is responsible
 * that the chunk has enough space.
 */
static void
ltc_perform_change(BTreeChunkDesc *chunk, OffsetNumber itemOffset,
				   BTreeChunkOperationType operation,
				   Pointer tupleHeader, OTuple tuple)
{
	BTreeTupleChunkDesc *tupleChunk = (BTreeTupleChunkDesc *) chunk;
	Pointer		itemPtr;
	uint16		tupleSize;

	if (operation == BTreeChunkOperationInsert ||
		operation == BTreeChunkOperationUpdate)
		tupleSize = chunk->ops->get_tuple_size(chunk->treeDesc, tuple);

	if (operation == BTreeChunkOperationInsert)
	{
		/* Allocate space for the new item, move other items if necessary */
		ltc_allocate_item(chunk, itemOffset,
						  chunk->ops->itemHeaderSize + MAXALIGN(tupleSize));

		/* Copy the new tuple and its header */
		itemPtr = ltc_get_item(tupleChunk, itemOffset);

		memcpy(itemPtr, tupleHeader, chunk->ops->itemHeaderSize);
		itemPtr += chunk->ops->itemHeaderSize;
		memcpy(itemPtr, tuple.data, tupleSize);

		ltc_set_item_flags(tupleChunk, itemOffset, tuple.formatFlags);
	}
	else if (operation == BTreeChunkOperationUpdate)
	{
		ltc_resize_item(chunk, itemOffset,
						chunk->ops->itemHeaderSize + MAXALIGN(tupleSize));

		/* Copy the new tuple and its header */
		itemPtr = ltc_get_item(tupleChunk, itemOffset);

		memcpy(itemPtr, tupleHeader, chunk->ops->itemHeaderSize);
		itemPtr += chunk->ops->itemHeaderSize;
		memcpy(itemPtr, tuple.data, tupleSize);

		ltc_set_item_flags(tupleChunk, itemOffset, tuple.formatFlags);
	}
	else if (operation == BTreeChunkOperationDelete)
	{
		ltc_delete_item(chunk, itemOffset);
	}
	else
		elog(ERROR, "invalid BTreeChunkOperationType: %d", operation);
}

/*
 * Compact the chunk by releasing space occupied by deleted tuples.  Currently
 * supports only leaf tuple chunks.
 */
static void
ltc_compact(BTreeChunkDesc *chunk)
{
	BTreeTupleChunkDesc *tupleChunk = (BTreeTupleChunkDesc *) chunk;
	uint16		itemsSize;
	Pointer		batchDst,
				batchSrc;
	OffsetNumber batchOffset = 0,
				lastOffset = 0;
	uint16		batchSize = 0;

	if (tupleChunk->chunkItemsCount == 0)
		return;

	/* Detect deleted items and delete them from the items array */
	for (OffsetNumber itemOffset = 0; itemOffset < tupleChunk->chunkItemsCount;
		 itemOffset++)
	{
		Pointer		header = NULL;
		OTuple		tuple = {0};
		bool		needsFree = false;

		chunk->ops->read_tuple(chunk, NULL, NULL, itemOffset,
							   &header, &tuple, &needsFree);

		if (((BTreeLeafTuphdr *) header)->deleted == BTreeLeafTupleNonDeleted)
			tupleChunk->chunkItems->items[lastOffset++] =
				tupleChunk->chunkItems->items[itemOffset];
	}
	tupleChunk->chunkItemsCount = lastOffset;

	/* All items were deleted, just exit */
	if (tupleChunk->chunkItemsCount == 0)
	{
		chunk->chunkDataSize = 0;
		return;
	}

	itemsSize = MAXALIGN(sizeof(LocationIndex) * tupleChunk->chunkItemsCount);
	batchDst = chunk->chunkData + itemsSize;

	/* Move items in batches in order to reduce number of memmove() calls */
	for (OffsetNumber itemOffset = 0; itemOffset < tupleChunk->chunkItemsCount;
		 itemOffset++)
	{
		Pointer		header = NULL;
		OTuple		tuple = {0};
		bool		needsFree = false;

		/*
		 * If there is a gap between the batch and the current item then move
		 * the batch and start new batch from the current item.
		 */
		if (itemOffset > 0 && ltc_get_item(tupleChunk, itemOffset) >
			(batchDst + batchSize))
		{
			batchSrc = ltc_get_item(tupleChunk, batchOffset);
			/* Do we actually need to move the batch? */
			if (batchDst != batchSrc)
			{
				uint16		dataShift = batchSrc - batchDst;

				memmove(batchDst, batchSrc, batchSize);

				/* Adjust the items array */
				for (OffsetNumber i = batchOffset; i < itemOffset; i++)
					tupleChunk->chunkItems->items[i] -= dataShift;
			}

			/* Update next batch offsets */
			batchDst += batchSize;
			batchOffset = itemOffset;
			batchSize = 0;
		}

		chunk->ops->read_tuple(chunk, NULL, NULL, itemOffset,
							   &header, &tuple, &needsFree);
		batchSize += chunk->ops->itemHeaderSize +
			MAXALIGN(chunk->ops->get_tuple_size(chunk->treeDesc, tuple));
	}

	/* Move the last batch */
	batchSrc = ltc_get_item(tupleChunk, batchOffset);
	if (batchDst != batchSrc)
	{
		uint16		dataShift = batchSrc - batchDst;

		memmove(batchDst, batchSrc, batchSize);

		/* Adjust the items array */
		for (OffsetNumber i = batchOffset; i < tupleChunk->chunkItemsCount; i++)
			tupleChunk->chunkItems->items[i] -= dataShift;

		chunk->chunkDataSize -= dataShift;
	}
}

/*
 * Compare the given key to a tuple at the given offset.  Partially load the
 * chunk from the page if necessary.
 */
static bool
ltc_cmp(BTreeChunkDesc *chunk, PartialPageState *partial, Page page,
		OffsetNumber itemOffset, void *key, BTreeKeyType keyType, int *result)
{
	BTreeTupleChunkDesc *tupleChunk = (BTreeTupleChunkDesc *) chunk PG_USED_FOR_ASSERTS_ONLY;
	Pointer		header2 = NULL;
	OTuple		tuple2 = {0};
	bool		needsFree = false;
	OBTreeKeyCmp cmpFunc = chunk->treeDesc->ops->cmp;

	if (partial && !ltc_load_partially(chunk, partial, page))
		return false;

	Assert(itemOffset < tupleChunk->chunkItemsCount);

	chunk->ops->read_tuple(chunk, NULL, NULL, itemOffset,
						   &header2, &tuple2, &needsFree);

	*result = cmpFunc(chunk->treeDesc, key, keyType,
					  &tuple2, chunk->ops->itemKeyType);
	return true;
}

/*
 * Search for a item by the given key.  Supports leaf and internal tuple chunks.
 */
static bool
ltc_search(BTreeChunkDesc *chunk, PartialPageState *partial, Page page,
		   void *key, BTreeKeyType keyType, OffsetNumber *itemOffset)
{
	BTreeTupleChunkDesc *tupleChunk = (BTreeTupleChunkDesc *) chunk;
	bool		isLeaf = chunk->ops->itemKeyType == BTreeKeyLeafTuple,
				nextKey;
	OffsetNumber low,
				mid,
				high;
	OBTreeKeyCmp cmpFunc = chunk->treeDesc->ops->cmp;
	int			targetCmpVal,
				result;

	if (partial && !ltc_load_partially(chunk, partial, page))
		return false;

	if (tupleChunk->chunkItemsCount == 0)
	{
		*itemOffset = 0;
		return true;
	}

	low = 0;
	high = tupleChunk->chunkItemsCount - 1;
	nextKey = (!isLeaf && keyType != BTreeKeyPageHiKey);

	/* Shouldn't look for hikey on leafs, because we're already here */
	Assert(!(isLeaf && keyType == BTreeKeyPageHiKey));

	/*
	 * Binary search to find the first key on the page >= `key`, or first page
	 * key > `key` when nextkey is true.
	 *
	 * For nextkey=false (cmp=1), the loop invariant is: all slots before
	 * `low` are < `key`, all slots at or after `high` are >= `key`.
	 *
	 * For nextkey=true (cmp=0), the loop invariant is: all slots before `low`
	 * are <= `key`, all slots at or after `high` are > `key`.
	 *
	 * We can fall out when `high` == `low`.
	 */
	high++;						/* establish the loop invariant for high */

	targetCmpVal = nextKey ? 0 : 1; /* a target value of cmpFunc() */

	/*
	 * Don't pass BTreeHiKey to comparison function, we've set nextkey flag
	 * instead.
	 */
	if (keyType == BTreeKeyPageHiKey)
		keyType = BTreeKeyNonLeafKey;

	while (high > low)
	{
		mid = low + ((high - low) / 2);

		if (!isLeaf && mid == 0 && *itemOffset == 0)
			result = 1;
		else
		{
			Pointer		midHeader;
			OTuple		midTuple;
			bool		needsFree;

			*itemOffset = mid;
			chunk->ops->read_tuple(chunk, NULL, NULL, *itemOffset,
								   &midHeader, &midTuple, &needsFree);

			result = cmpFunc(chunk->treeDesc, key, keyType, &midTuple,
							 chunk->ops->itemKeyType);
		}

		if (result >= targetCmpVal)
			low = mid + 1;
		else
			high = mid;
	}

	*itemOffset = low;

	return true;
}

/*
 * Read a chunk item by the given offset.  Supports leaf and internal tuple chunks.
 */
static bool
ltc_read_tuple(BTreeChunkDesc *chunk, PartialPageState *partial, Page page,
			   OffsetNumber itemOffset, Pointer *tupleHeader, OTuple *tuple,
			   bool *needsFree)
{
	BTreeTupleChunkDesc *tupleChunk = (BTreeTupleChunkDesc *) chunk PG_USED_FOR_ASSERTS_ONLY;
	Pointer		item;

	if (partial && !ltc_load_partially(chunk, partial, page))
		return false;

	Assert(itemOffset < tupleChunk->chunkItemsCount);

	item = ltc_get_item(tupleChunk, itemOffset);

	*tupleHeader = item;
	tuple->data = item + chunk->ops->itemHeaderSize;
	tuple->formatFlags = ltc_get_item_flags(tupleChunk, itemOffset);

	/* Set always to true in the current implementation */
	*needsFree = true;

	return true;
}

/*
 * Initialize leaf tuple chunk builder.
 */
static void
ltc_builder_init(BTreeChunkBuilder *chunkBuilder)
{
	chunkBuilder->chunkItemsCount = 0;
}

/*
 * Estimate the size shift of the result chunk after the operation.
 */
static int32
ltc_builder_estimate(BTreeChunkBuilder *chunkBuilder, OTuple tuple)
{
	LocationIndex itemsShift;

	itemsShift = MAXALIGN(sizeof(LocationIndex) * (chunkBuilder->chunkItemsCount + 1)) -
		MAXALIGN(sizeof(LocationIndex) * chunkBuilder->chunkItemsCount);

	return chunkBuilder->ops->itemHeaderSize +
		MAXALIGN(chunkBuilder->ops->get_tuple_size(chunkBuilder->treeDesc, tuple)) +
		itemsShift;
}

/*
 * Copy the tuple to the chunk builder buffer.
 */
static void
ltc_builder_add(BTreeChunkBuilder *chunkBuilder, BTreeChunkBuilderItem *chunkItem)
{
	Assert(chunkBuilder->chunkItemsCount < BTREE_PAGE_MAX_CHUNK_ITEMS);

	chunkBuilder->chunkItems[chunkBuilder->chunkItemsCount++] = *chunkItem;
}

/*
 * Finalizer the builder and copy tuples to the new chunk offset.
 */
static void
ltc_builder_finish(BTreeChunkBuilder *chunkBuilder, BTreeChunkDesc *chunk)
{
	BTreeTupleChunkDesc *tupleChunk = (BTreeTupleChunkDesc *) chunk;
	uint16		itemsShift,
				dataShift = 0;
	Pointer		ptr;

	chunk->treeDesc = chunkBuilder->treeDesc;
	tupleChunk->chunkItemsCount = chunkBuilder->chunkItemsCount;

	itemsShift = MAXALIGN(sizeof(LocationIndex) * tupleChunk->chunkItemsCount);
	for (int i = 0; i < chunkBuilder->chunkItemsCount; i++)
		dataShift += chunkBuilder->chunkItems[i].size;

	chunk->chunkDataSize = itemsShift + dataShift;
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
	for (OffsetNumber i = 0; i < tupleChunk->chunkItemsCount; i++)
	{
		tupleChunk->chunkItems->items[i] = itemsShift;
		ltc_set_item_flags(tupleChunk, i, chunkBuilder->chunkItems[i].flags);

		itemsShift += chunkBuilder->chunkItems[i].size;
	}
}

static inline uint16
ltc_get_tuple_size(BTreeDescr *treeDesc, OTuple tuple)
{
	return o_btree_len(treeDesc, tuple, OTupleLength);
}

/*
 * Implementation of internal tuple chunks.
 */

static inline uint16
itc_get_tuple_size(BTreeDescr *treeDesc, OTuple tuple)
{
	return o_btree_len(treeDesc, tuple, OKeyLength);
}

/*
 * Implementation of hikey chunks.
 */

static inline uint8
htc_get_item_flags(BTreeHiKeyChunkDesc *chunk, OffsetNumber itemOffset)
{
	Assert(itemOffset < chunk->chunkItemsCount);

	return ITEM_GET_FLAGS(chunk->chunkItems->items[itemOffset]);
}

static inline void
htc_set_item_flags(BTreeHiKeyChunkDesc *chunk, OffsetNumber itemOffset,
				   uint8 flags)
{
	Assert(itemOffset < chunk->chunkItemsCount);

	chunk->chunkItems->items[itemOffset] =
		ITEM_SET_FLAGS(chunk->chunkItems->items[itemOffset], flags);
}

static inline Pointer
htc_get_item(BTreeHiKeyChunkDesc *chunk, OffsetNumber itemOffset)
{
	Assert(itemOffset < chunk->chunkItemsCount);

	return ((BTreeChunkDesc *) chunk)->chunkData +
		ITEM_GET_OFFSET(chunk->chunkItems->items[itemOffset]);
}

/*
 * Get size of the item as a substraction of offset of the next item and the
 * target.
 */
static inline uint16
htc_get_item_size(BTreeHiKeyChunkDesc *chunk, OffsetNumber itemOffset)
{
	LocationIndex itemLocation,
				nextItemLocation;

	Assert(chunk->chunkItemsCount > itemOffset);

	/* Calculate offset form the beginning of the chunk */
	itemLocation = ITEM_GET_OFFSET(chunk->chunkItems->items[itemOffset]);

	if (itemOffset + 1 < chunk->chunkItemsCount)
	{
		nextItemLocation = ITEM_GET_OFFSET(chunk->chunkItems->items[itemOffset + 1]);
		return nextItemLocation - itemLocation;
	}
	else
		return ((BTreeChunkDesc *) chunk)->chunkDataSize - itemLocation;
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

	/* Calculate the shift of the data after new item is inserted */
	dataShift = itemSize;

	if (itemOffset < hikeyChunk->chunkItemsCount)
		itemPtr = htc_get_item(hikeyChunk, itemOffset);
	else
	{
		Assert(itemOffset == hikeyChunk->chunkItemsCount);

		itemPtr = chunk->chunkData + chunk->chunkDataSize;
	}

	endPtr = chunk->chunkData + chunk->chunkDataSize;

	/* Data should still fit to the page */
	Assert(chunk->chunkDataSize + dataShift <= ORIOLEDB_BLCKSZ);

	/* Shift the data after insert location */
	Assert(itemPtr <= endPtr);

	memmove(itemPtr + dataShift, itemPtr, endPtr - itemPtr);

	hikeyChunk->chunkItemsCount++;
	chunk->chunkDataSize += dataShift;

	/* Add the new item to the items array  */
	for (OffsetNumber i = hikeyChunk->chunkItemsCount; i > itemOffset; i--)
		hikeyChunk->chunkItems->items[i] = hikeyChunk->chunkItems->items[i - 1] + dataShift;
	hikeyChunk->chunkItems->items[itemOffset] = itemPtr - chunk->chunkData;
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

	Assert(itemOffset < hikeyChunk->chunkItemsCount);
	if (itemOffset + 1 < hikeyChunk->chunkItemsCount)
		nextItemPtr = htc_get_item(hikeyChunk, itemOffset + 1);
	else
		nextItemPtr = chunk->chunkData + chunk->chunkDataSize;

	endPtr = chunk->chunkData + chunk->chunkDataSize;

	/* Data should still fit to the page */
	Assert(chunk->chunkDataSize + dataShift <= ORIOLEDB_BLCKSZ);

	/* Shift the data after the item */
	memmove(nextItemPtr + dataShift, nextItemPtr, endPtr - nextItemPtr);

	/* Adjust the items array */
	for (OffsetNumber i = itemOffset + 1; i < hikeyChunk->chunkItemsCount; i++)
		hikeyChunk->chunkItems->items[i] += dataShift;

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

	Assert(hikeyChunk->chunkItemsCount > 0);

	/* Calculate the shift of the data after the item is deleted */
	dataShift = itemSize;

	Assert(itemOffset < hikeyChunk->chunkItemsCount);
	itemPtr = htc_get_item(hikeyChunk, itemOffset);

	endPtr = chunk->chunkData + chunk->chunkDataSize;
	Assert(endPtr - dataShift >= itemPtr);

	/*
	 * Adjust the items array.
	 */
	for (OffsetNumber i = itemOffset; i < hikeyChunk->chunkItemsCount - 1; i++)
		hikeyChunk->chunkItems->items[i] = hikeyChunk->chunkItems->items[i + 1] - dataShift;

	/* Move the data after deleted item */
	memmove(itemPtr, itemPtr + itemSize, endPtr - itemPtr - itemSize);

	hikeyChunk->chunkItemsCount--;
	chunk->chunkDataSize -= dataShift;
}

/*
 * Initialize hikey tuples chunk and copy the items array.
 */
static void
htc_init(BTreeChunkDesc *chunk, Pointer chunkData,
		 uint16 chunkDataSize, BTreeChunkItem *chunkItems,
		 uint16 chunkItemsCount, OffsetNumber chunkOffset)
{
	BTreeHiKeyChunkDesc *hikeyChunk = (BTreeHiKeyChunkDesc *) chunk;

	Assert(chunkItemsCount <= BTREE_PAGE_MAX_CHUNKS);

	chunk->chunkData = chunkData;
	chunk->chunkDataSize = chunkDataSize;

	hikeyChunk->chunkItemsCount = chunkItemsCount;
	memcpy(hikeyChunk->chunkItems, chunkItems, sizeof(LocationIndex) *
		   hikeyChunk->chunkItemsCount);
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
	BTreeHiKeyChunkDesc *hikeyChunk = (BTreeHiKeyChunkDesc *) chunk PG_USED_FOR_ASSERTS_ONLY;
	Pointer		header2 = NULL;
	OTuple		tuple2 = {0};
	bool		needsFree = false;
	OBTreeKeyCmp cmpFunc = chunk->treeDesc->ops->cmp;

	Assert(itemOffset < hikeyChunk->chunkItemsCount);

	chunk->ops->read_tuple(chunk, NULL, NULL, itemOffset,
						   &header2, &tuple2, &needsFree);

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
	BTreeHiKeyChunkDesc *hikeyChunk = (BTreeHiKeyChunkDesc *) chunk;
	OffsetNumber mid,
				low,
				high;
	int			targetCmpVal,
				result;
	bool		nextkey;
	OBTreeKeyCmp cmpFunc = chunk->treeDesc->ops->cmp;

	Assert(hikeyChunk->chunkItemsCount > 0);

	low = 0;
	high = hikeyChunk->chunkItemsCount - 1;
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
		Pointer		midHeader;
		OTuple		midTuple;
		bool		needsFree;

		mid = low + ((high - low) / 2);

		Assert(mid < hikeyChunk->chunkItemsCount - 1);

		chunk->ops->read_tuple(chunk, NULL, NULL, mid,
							   &midHeader, &midTuple, &needsFree);

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

	Assert(itemOffset < hikeyChunk->chunkItemsCount);

	*tupleHeader = NULL;
	tuple->data = htc_get_item(hikeyChunk, itemOffset);
	tuple->formatFlags = htc_get_item_flags(hikeyChunk, itemOffset);

	/* Set always to true in the current implementation */
	*needsFree = true;

	return true;
}

const BTreeChunkOps LeafTupleChunkOps = {
	.chunkDescSize = sizeof(BTreeTupleChunkDesc),
	.itemHeaderSize = BTreeLeafTuphdrSize,
	.itemKeyType = BTreeKeyLeafTuple,
	/* Main functions */
	.init = ltc_init,
	.estimate_change = ltc_estimate_change,
	.perform_change = ltc_perform_change,
	.compact = ltc_compact,
	.cmp = ltc_cmp,
	.search = ltc_search,
	.read_tuple = ltc_read_tuple,
	/* Builder functions */
	.builder_init = ltc_builder_init,
	.builder_estimate = ltc_builder_estimate,
	.builder_add = ltc_builder_add,
	.builder_finish = ltc_builder_finish,
	/* Utility functions */
	.get_tuple_size = ltc_get_tuple_size
};

const BTreeChunkOps InternalTupleChunkOps = {
	.chunkDescSize = sizeof(BTreeTupleChunkDesc),
	.itemHeaderSize = BTreeNonLeafTuphdrSize,
	.itemKeyType = BTreeKeyNonLeafKey,
	/* Main functions */
	.init = ltc_init,
	.estimate_change = ltc_estimate_change,
	.perform_change = ltc_perform_change,
	.compact = NULL,
	.cmp = ltc_cmp,
	.search = ltc_search,
	.read_tuple = ltc_read_tuple,
	/* Builder functions */
	.builder_init = ltc_builder_init,
	.builder_estimate = ltc_builder_estimate,
	.builder_add = ltc_builder_add,
	.builder_finish = ltc_builder_finish,
	/* Utility functions */
	.get_tuple_size = itc_get_tuple_size
};

const BTreeChunkOps HiKeyChunkOps = {
	.chunkDescSize = sizeof(BTreeHiKeyChunkDesc),
	.itemHeaderSize = 0,
	.itemKeyType = BTreeKeyNonLeafKey,
	/* Main functions */
	.init = htc_init,
	.estimate_change = htc_estimate_change,
	.perform_change = htc_perform_change,
	.compact = NULL,
	.cmp = htc_cmp,
	.search = htc_search,
	.read_tuple = htc_read_tuple,
	/* Builder functions */
	.builder_init = NULL,
	.builder_estimate = NULL,
	.builder_add = NULL,
	.builder_finish = NULL,
	/* Utility functions */
	.get_tuple_size = htc_get_tuple_size
};
