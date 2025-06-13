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

const BTreeChunkOps BTreeLeafTupleChunkOps;
const BTreeChunkOps BTreeInternalTupleChunkOps;

/*
 * Implementation of leaf tuple chunks.
 */

static inline uint16
ltc_get_chunk_items_count(BTreeChunkDesc *chunk)
{
	BTreePageHeader *header = (BTreePageHeader *) chunk->page;

	if (chunk->chunkOffset + 1 < header->chunksCount)
		return header->chunkDesc[chunk->chunkOffset + 1].offset -
			header->chunkDesc[chunk->chunkOffset].offset;
	else
		return header->itemsCount - header->chunkDesc[chunk->chunkOffset].offset;
}

static inline Pointer
ltc_get_chunk_start(BTreeChunkDesc *chunk)
{
	BTreePageHeader *header = (BTreePageHeader *) chunk->page;

	Assert(chunk->chunkOffset < header->chunksCount);

	return (Pointer) chunk->page +
		SHORT_GET_LOCATION(header->chunkDesc[chunk->chunkOffset].shortLocation);
}

static inline uint16
ltc_get_chunk_size(BTreeChunkDesc *chunk)
{
	BTreePageHeader *header = (BTreePageHeader *) chunk->page;
	LocationIndex	chunkLocation;

	Assert(chunk->chunkOffset < header->chunksCount);

	chunkLocation = SHORT_GET_LOCATION(header->chunkDesc[chunk->chunkOffset].shortLocation);

	if (chunk->chunkOffset + 1 < header->chunksCount)
		return SHORT_GET_LOCATION(header->chunkDesc[chunk->chunkOffset + 1].shortLocation) -
			chunkLocation;
	else
		return header->dataSize - chunkLocation;
}

static inline uint8
ltc_get_item_flags(BTreeChunkDesc *chunk, OffsetNumber itemOffset)
{
	LocationIndex *items = (LocationIndex *) ltc_get_chunk_start(chunk);

	Assert(itemOffset < ltc_get_chunk_items_count(chunk));

	return ITEM_GET_FLAGS(items[itemOffset]);
}

static inline void
ltc_set_item_flags(BTreeChunkDesc *chunk, OffsetNumber itemOffset,
				   uint8 flags)
{
	LocationIndex *items = (LocationIndex *) ltc_get_chunk_start(chunk);

	Assert(itemOffset < ltc_get_chunk_items_count(chunk));

	items[itemOffset] = ITEM_SET_FLAGS(items[itemOffset], flags);
}

static inline Pointer
ltc_get_item(BTreeChunkDesc *chunk, OffsetNumber itemOffset)
{
	Pointer		chunkStart = ltc_get_chunk_start(chunk);

	Assert(itemOffset < ltc_get_chunk_items_count(chunk));

	return chunkStart +
		ITEM_GET_OFFSET(((LocationIndex *) chunkStart)[itemOffset]);
}

/*
 * Get size of the item as a substraction of offset of the next item and the
 * target.
 */
static inline uint16
ltc_get_item_size(BTreeChunkDesc *chunk, OffsetNumber itemOffset)
{
	uint16		chunkItemsCount = ltc_get_chunk_items_count(chunk);
	LocationIndex *items = (LocationIndex *) ltc_get_chunk_start(chunk);
	LocationIndex itemLocation;

	Assert(itemOffset < chunkItemsCount);

	/* Calculate offset form the beginning of the chunk */
	itemLocation = ITEM_GET_OFFSET(items[itemOffset]);

	if (itemOffset + 1 < chunkItemsCount)
		return ITEM_GET_OFFSET(items[itemOffset + 1]) -
			itemLocation;
	else
		return ltc_get_chunk_size(chunk) - itemLocation;
}

/*
 * Allocate space for the new item in the chunk.  A caller is responsible to
 * copy the item afterwards.
 */
static void
ltc_allocate_item(BTreeChunkDesc *chunk, OffsetNumber itemOffset,
				  uint16 itemSize)
{
	BTreePageHeader *header = (BTreePageHeader *) chunk->page;
	uint16		chunkItemsCount = ltc_get_chunk_items_count(chunk);
	Pointer		chunkStart = ltc_get_chunk_start(chunk);
	uint16		chunkSize = ltc_get_chunk_size(chunk);
	LocationIndex *items = (LocationIndex *) ltc_get_chunk_start(chunk);
	LocationIndex itemsShift,
				dataShift;
	Pointer		firstItemPtr,
				itemPtr,
				endPtr;

	Assert(itemSize == MAXALIGN(itemSize));

	/* Calculate the change of (maxaligned) item array size */
	itemsShift = MAXALIGN(sizeof(LocationIndex) * (chunkItemsCount + 1)) -
		MAXALIGN(sizeof(LocationIndex) * chunkItemsCount);

	/* Calculate the shift of the data after new item is inserted */
	dataShift = itemsShift + itemSize;
	Assert(dataShift == MAXALIGN(dataShift));

	if (chunkItemsCount > 0)
		firstItemPtr = ltc_get_item(chunk, 0);
	else
		firstItemPtr = chunkStart +
			MAXALIGN(sizeof(LocationIndex) * chunkItemsCount);

	if (itemOffset < chunkItemsCount)
		itemPtr = ltc_get_item(chunk, itemOffset);
	else
	{
		Assert(itemOffset == chunkItemsCount);

		itemPtr = chunkStart + chunkSize;
	}

	endPtr = chunkStart + chunkSize;

	/* Data should still fit to the page */
	Assert(chunkSize + dataShift <= ORIOLEDB_BLCKSZ);

	/* Shift the data after insert location */
	Assert(itemPtr <= endPtr);

	memmove(itemPtr + dataShift, itemPtr, endPtr - itemPtr);

	/* Adjust chunks parameters */
	for (int i = chunk->chunkOffset + 1; i < header->chunksCount; i++)
		header->chunkDesc[i].offset++;
	header->itemsCount++;
	/*
	 * Adjust size in the header only for the last chunk. For other chunks it is
	 * a users responsibility to move other chunks.
	 */
	if (chunk->chunkOffset == header->chunksCount - 1)
		header->dataSize += dataShift;

	chunkItemsCount++;
	chunkSize += dataShift;

	if (itemsShift != 0)
	{
		/*
		 * If items array size is changed, then we have to also move the items
		 * before insert location and adjust those locations in the items
		 * array.
		 */
		memmove(firstItemPtr + itemsShift, firstItemPtr, itemPtr - firstItemPtr);
		for (OffsetNumber i = 0; i < itemOffset; i++)
			items[i] += itemsShift;
	}

	/* Add the new item to the items array  */
	for (int i = chunkItemsCount - 1; i > itemOffset; i--)
		items[i] = items[i - 1] + dataShift;
	items[itemOffset] = itemPtr - chunkStart + itemsShift;
}

/*
 * Resize the chunk to fit the new item size.  A caller is responsible to copy
 * the new item.
 */
static void
ltc_resize_item(BTreeChunkDesc *chunk, OffsetNumber itemOffset,
				uint16 newItemSize)
{
	BTreePageHeader *header = (BTreePageHeader *) chunk->page;
	uint16		chunkItemsCount = ltc_get_chunk_items_count(chunk);
	Pointer		chunkStart = ltc_get_chunk_start(chunk);
	uint16		chunkSize = ltc_get_chunk_size(chunk);
	LocationIndex *items = (LocationIndex *) ltc_get_chunk_start(chunk);
	int32		dataShift;
	Pointer		nextItemPtr,
				endPtr;

	Assert(newItemSize == MAXALIGN(newItemSize));

	/* Calculate the shift of the data after the item resize */
	dataShift = newItemSize - ltc_get_item_size(chunk, itemOffset);
	Assert(dataShift == MAXALIGN(dataShift));

	/* We don't move items to fill the gap if the new tuple is smaller */
	if (dataShift <= 0)
		return;

	Assert(itemOffset < chunkItemsCount);
	if (itemOffset + 1 < chunkItemsCount)
		nextItemPtr = ltc_get_item(chunk, itemOffset + 1);
	else
		nextItemPtr = chunkStart + chunkSize;

	endPtr = chunkStart + chunkSize;

	/* Data should still fit to the page */
	Assert(chunkSize + dataShift <= ORIOLEDB_BLCKSZ);

	/* Shift the data after the item */
	memmove(nextItemPtr + dataShift, nextItemPtr, endPtr - nextItemPtr);

	header->dataSize += dataShift;

	chunkSize += dataShift;

	/* Adjust the items array */
	for (int i = itemOffset + 1; i < chunkItemsCount; i++)
		items[i] += dataShift;
}

/*
 * Delete the item from the chunk by the provided offset.
 */
static void
ltc_delete_item(BTreeChunkDesc *chunk, OffsetNumber itemOffset)
{
	BTreePageHeader *header = (BTreePageHeader *) chunk->page;
	uint16		chunkItemsCount = ltc_get_chunk_items_count(chunk);
	Pointer		chunkStart = ltc_get_chunk_start(chunk);
	uint16		chunkSize = ltc_get_chunk_size(chunk);
	LocationIndex *items = (LocationIndex *) ltc_get_chunk_start(chunk);
	uint16		itemSize;
	LocationIndex itemsShift,
				dataShift;
	Pointer		firstItemPtr,
				itemPtr,
				endPtr;

	itemSize = ltc_get_item_size(chunk, itemOffset);
	Assert(itemSize == MAXALIGN(itemSize));

	Assert(chunkItemsCount > 0);

	/* Calculate the change of (maxaligned) item array size */
	itemsShift = MAXALIGN(sizeof(LocationIndex) * chunkItemsCount) -
		MAXALIGN(sizeof(LocationIndex) * (chunkItemsCount - 1));

	/* Calculate the shift of the data after the item is deleted */
	dataShift = itemsShift + itemSize;
	Assert(dataShift == MAXALIGN(dataShift));

	firstItemPtr = ltc_get_item(chunk, 0);

	Assert(itemOffset < chunkItemsCount);
	itemPtr = ltc_get_item(chunk, itemOffset);

	endPtr = chunkStart + chunkSize;
	Assert(endPtr - dataShift >= itemPtr - itemsShift);

	/*
	 * Adjust the items array.  We should do this first to prevent it been
	 * overridden by the data when it's shorten.
	 */
	for (int i = itemOffset; i < chunkItemsCount - 1; i++)
		items[i] = items[i + 1] - dataShift;

	if (itemsShift != 0)
	{
		/* Shift the data before deleted item when items arrays is shorten. */
		memmove(firstItemPtr - itemsShift, firstItemPtr, itemPtr - firstItemPtr);

		/* Shift item pointers of those items */
		for (OffsetNumber i = 0; i < itemOffset; i++)
			items[i] -= itemsShift;
	}

	/* Move the data after deleted item */
	memmove(itemPtr - itemsShift, itemPtr + itemSize, endPtr - itemPtr - itemSize);

	header->itemsCount--;
	header->dataSize -= dataShift;
}

static bool
ltc_load_partially(BTreeChunkDesc *chunk, PartialPageState *partial, Page page)
{
	BTreePageHeader *header = (BTreePageHeader *) page;
	Pointer		chunkStart = ltc_get_chunk_start(chunk);
	uint16		chunkSize = ltc_get_chunk_size(chunk);
	Page		sourcePage = partial->src;
	uint32		pageState,
				sourceState;
	LocationIndex chunkLocation =
		SHORT_GET_LOCATION(header->chunkDesc[chunk->chunkOffset].shortLocation);

	Assert(partial != NULL);

	if (!partial->isPartial || partial->chunkIsLoaded[chunk->chunkOffset])
		return true;

	/* chunk->ops->init(chunk, page, chunk->chunkOffset); */

	memcpy(chunkStart, (Pointer) sourcePage + chunkLocation, chunkSize);

	pg_read_barrier();

	pageState = pg_atomic_read_u32(&(O_PAGE_HEADER(page)->state));
	sourceState = pg_atomic_read_u32(&(O_PAGE_HEADER(sourcePage)->state));
	if ((pageState & PAGE_STATE_CHANGE_COUNT_MASK) != (sourceState & PAGE_STATE_CHANGE_COUNT_MASK) ||
		O_PAGE_STATE_READ_IS_BLOCKED(sourceState))
		return false;

	if (O_PAGE_GET_CHANGE_COUNT(page) != O_PAGE_GET_CHANGE_COUNT(sourcePage))
		return false;

	partial->chunkIsLoaded[chunk->chunkOffset] = true;

	return true;
}

/*
 * Initialize leaf tuple chunk.  Directly use passed pointers to the chunk and
 * the chunk items array as is.
 */
static void
ltc_init(BTreeChunkDesc *chunk, Page page, OffsetNumber chunkOffset)
{
	BTreePageHeader *header = (BTreePageHeader *) page;

	Assert(chunkOffset < header->chunksCount);

	chunk->chunkOffset = chunkOffset;
	chunk->page = page;
}

/*
 * Estimate size shift of the operation over the leaf tuple chunk.
 */
static int32
ltc_estimate_change(BTreeChunkDesc *chunk, OffsetNumber itemOffset,
					BTreeChunkOperationType operation, OTuple tuple)
{
	uint16		chunkItemsCount = ltc_get_chunk_items_count(chunk);
	int32		itemsShift = 0,
				sizeNeeded = 0;

	if (operation == BTreeChunkOperationInsert)
	{
		/* Calculate the change of (maxaligned) item array size */
		itemsShift = MAXALIGN(sizeof(LocationIndex) * (chunkItemsCount + 1)) -
			MAXALIGN(sizeof(LocationIndex) * chunkItemsCount);

		sizeNeeded = chunk->ops->itemHeaderSize +
			MAXALIGN(o_btree_len(chunk->treeDesc, tuple, chunk->ops->itemLengthType)) +
			itemsShift;
	}
	else if (operation == BTreeChunkOperationUpdate)
	{
		sizeNeeded = (chunk->ops->itemHeaderSize +
					  MAXALIGN(o_btree_len(chunk->treeDesc, tuple,
										   chunk->ops->itemLengthType))) -
			ltc_get_item_size(chunk, itemOffset);

		/* We don't move items to fill the gap if the new tuple is smaller */
		if (sizeNeeded < 0)
			sizeNeeded = 0;
	}
	else if (operation == BTreeChunkOperationDelete)
	{
		/* Calculate the change of (maxaligned) item array size */
		itemsShift = MAXALIGN(sizeof(LocationIndex) * chunkItemsCount) -
			MAXALIGN(sizeof(LocationIndex) * (chunkItemsCount - 1));

		sizeNeeded = (-1) * (ltc_get_item_size(chunk, itemOffset) +
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
	Pointer		itemPtr;
	uint16		tupleSize;

	if (operation == BTreeChunkOperationInsert ||
		operation == BTreeChunkOperationUpdate)
		tupleSize = o_btree_len(chunk->treeDesc, tuple,
								chunk->ops->itemLengthType);

	if (operation == BTreeChunkOperationInsert)
	{
		/* Allocate space for the new item, move other items if necessary */
		ltc_allocate_item(chunk, itemOffset,
						  chunk->ops->itemHeaderSize + MAXALIGN(tupleSize));

		/* Copy the new tuple and its header */
		itemPtr = ltc_get_item(chunk, itemOffset);

		memcpy(itemPtr, tupleHeader, chunk->ops->itemHeaderSize);
		itemPtr += chunk->ops->itemHeaderSize;
		memcpy(itemPtr, tuple.data, tupleSize);

		ltc_set_item_flags(chunk, itemOffset, tuple.formatFlags);
	}
	else if (operation == BTreeChunkOperationUpdate)
	{
		ltc_resize_item(chunk, itemOffset,
						chunk->ops->itemHeaderSize + MAXALIGN(tupleSize));

		/* Copy the new tuple and its header */
		itemPtr = ltc_get_item(chunk, itemOffset);

		memcpy(itemPtr, tupleHeader, chunk->ops->itemHeaderSize);
		itemPtr += chunk->ops->itemHeaderSize;
		memcpy(itemPtr, tuple.data, tupleSize);

		ltc_set_item_flags(chunk, itemOffset, tuple.formatFlags);
	}
	else if (operation == BTreeChunkOperationDelete)
	{
		Assert(tupleHeader == NULL);

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
	BTreePageHeader *header = (BTreePageHeader *) chunk->page;
	uint16		chunkItemsCount = ltc_get_chunk_items_count(chunk);
	LocationIndex *items = (LocationIndex *) ltc_get_chunk_start(chunk);
	uint16		itemsSize;
	Pointer		batchDst,
				batchSrc;
	OffsetNumber batchOffset = 0,
				lastOffset = 0;
	uint16		batchSize = 0;

	if (unlikely(chunkItemsCount == 0))
		return;

	/* Detect deleted items and delete them from the items array */
	for (int itemOffset = 0; itemOffset < chunkItemsCount; itemOffset++)
	{
		Pointer		tupleHeader = NULL;
		OTuple		tuple = {0};
		bool		isCopy = false;

		chunk->ops->read_tuple(chunk, NULL, NULL, itemOffset,
							   &tupleHeader, &tuple, &isCopy);

		/* TODO: Consider CommitSeqNo */
		if (((BTreeLeafTuphdr *) tupleHeader)->deleted == BTreeLeafTupleNonDeleted)
			items[lastOffset++] = items[itemOffset];
	}

	/* Adjust chunks parameters */
	for (int i = chunk->chunkOffset + 1; i < header->chunksCount; i++)
	{
		header->chunkDesc[i].shortLocation += LOCATION_GET_SHORT(dataShift);
		header->chunkDesc[i].offset++;
	}
	header->itemsCount++;
	chunk->chunkItemsCount = lastOffset;

	/* All items were deleted, just exit */
	if (unlikely(chunk->chunkItemsCount == 0))
	{
		chunk->chunkDataSize = 0;
		return;
	}

	itemsSize = MAXALIGN(sizeof(LocationIndex) * chunk->chunkItemsCount);
	batchDst = chunk->chunkData + itemsSize;

	/* Move items in batches in order to reduce number of memmove() calls */
	for (OffsetNumber itemOffset = 0; itemOffset < chunk->chunkItemsCount;
		 itemOffset++)
	{
		Pointer		tupleHeader = NULL;
		OTuple		tuple = {0};
		bool		isCopy = false;

		/*
		 * If there is a gap between the batch and the current item then move
		 * the batch and start new batch from the current item.
		 */
		if (itemOffset > 0 && ltc_get_item(chunk, itemOffset) >
			(batchDst + batchSize))
		{
			batchSrc = ltc_get_item(chunk, batchOffset);
			/* Do we actually need to move the batch? */
			if (batchDst != batchSrc)
			{
				uint16		dataShift = batchSrc - batchDst;

				memmove(batchDst, batchSrc, batchSize);

				/* Adjust the items array */
				for (OffsetNumber i = batchOffset; i < itemOffset; i++)
					chunk->chunkItems.tupleItems[i] -= dataShift;
			}

			/* Update next batch offsets */
			batchDst += batchSize;
			batchOffset = itemOffset;
			batchSize = 0;
		}

		chunk->ops->read_tuple(chunk, NULL, NULL, itemOffset,
							   &tupleHeader, &tuple, &isCopy);
		batchSize += chunk->ops->itemHeaderSize +
			MAXALIGN(o_btree_len(chunk->treeDesc, tuple,
								 chunk->ops->itemLengthType));
	}

	/* Move the last batch */
	batchSrc = ltc_get_item(chunk, batchOffset);
	if (batchDst != batchSrc)
	{
		uint16		dataShift = batchSrc - batchDst;

		memmove(batchDst, batchSrc, batchSize);

		/* Adjust the items array */
		for (OffsetNumber i = batchOffset; i < chunk->chunkItemsCount; i++)
			chunk->chunkItems.tupleItems[i] -= dataShift;

		chunk->chunkDataSize -= dataShift;
	}
}

/*
 * Get available size for compaction.
 */
static uint16
ltc_get_available_size(BTreeChunkDesc *chunk, CommitSeqNo csn)
{
	uint16		available_size = 0;

	if (unlikely(chunk->chunkItemsCount == 0))
		return 0;

	for (OffsetNumber itemOffset = 0; itemOffset < chunk->chunkItemsCount;
		 itemOffset++)
	{
		BTreeLeafTuphdr *header = NULL;
		OTuple		tuple = {0};
		bool		isCopy = false;

		chunk->ops->read_tuple(chunk, NULL, NULL, itemOffset,
							   (Pointer *) &header, &tuple, &isCopy);

		if (XACT_INFO_FINISHED_FOR_EVERYBODY(header->xactInfo))
		{
			if (header->deleted)
			{
				if (COMMITSEQNO_IS_INPROGRESS(csn) ||
					XACT_INFO_MAP_CSN(header->xactInfo) < csn)
					available_size += ltc_get_item_size((BTreeChunkDesc *) chunk,
														itemOffset);
			}
			else
			{
				available_size += ltc_get_item_size((BTreeChunkDesc *) chunk,
													itemOffset) -
					(chunk->ops->itemHeaderSize +
					 MAXALIGN(o_btree_len(chunk->treeDesc, tuple,
										  chunk->ops->itemLengthType)));
			}
		}
	}

	return available_size;
}

/*
 * Compare the given key to a tuple at the given offset.  Partially load the
 * chunk from the page if necessary.
 */
static bool
ltc_cmp(BTreeChunkDesc *chunk, PartialPageState *partial, Page page,
		OffsetNumber itemOffset, void *key, BTreeKeyType keyType, int *result)
{
	Pointer		header2 = NULL;
	OTuple		tuple2 = {0};
	bool		isCopy = false;
	OBTreeKeyCmp cmpFunc = chunk->treeDesc->ops->cmp;

	if (partial && !ltc_load_partially(chunk, partial, page))
		return false;

	Assert(itemOffset < chunk->chunkItemsCount);

	chunk->ops->read_tuple(chunk, NULL, NULL, itemOffset,
						   &header2, &tuple2, &isCopy);

	Assert(!isCopy);

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

	if (chunk->chunkItemsCount == 0)
	{
		*itemOffset = 0;
		return true;
	}

	low = 0;
	high = chunk->chunkItemsCount - 1;
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
			bool		isCopy;

			*itemOffset = mid;
			chunk->ops->read_tuple(chunk, NULL, NULL, *itemOffset,
								   &midHeader, &midTuple, &isCopy);

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
			   bool *isCopy)
{
	Pointer		item;

	if (partial && !ltc_load_partially(chunk, partial, page))
		return false;

	Assert(itemOffset < chunk->chunkItemsCount);

	item = ltc_get_item(chunk, itemOffset);

	*tupleHeader = item;
	tuple->data = item + chunk->ops->itemHeaderSize;
	tuple->formatFlags = ltc_get_item_flags(chunk, itemOffset);

	/* Set always to false in the current implementation */
	*isCopy = false;

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
		MAXALIGN(o_btree_len(chunkBuilder->treeDesc, tuple,
							 chunkBuilder->ops->itemLengthType)) +
		itemsShift;
}

/*
 * Copy the pointer to the tuple to the chunk builder buffer.
 */
static void
ltc_builder_add(BTreeChunkBuilder *chunkBuilder, BTreeChunkBuilderItem *chunkItem)
{
	Assert(chunkBuilder->chunkItemsCount < BTREE_PAGE_MAX_CHUNK_ITEMS);

	chunkBuilder->chunkItems[chunkBuilder->chunkItemsCount++] = *chunkItem;
}

/*
 * Finalizer the builder and copy tuples to the new chunk buffer.
 */
static void
ltc_builder_finish(BTreeChunkBuilder *chunkBuilder, BTreeChunkDesc *chunk)
{
	uint16		itemsShift,
				dataShift = 0;
	Pointer		ptr;

	chunk->treeDesc = chunkBuilder->treeDesc;
	chunk->chunkItemsCount = chunkBuilder->chunkItemsCount;

	itemsShift = MAXALIGN(sizeof(LocationIndex) * chunk->chunkItemsCount);
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
	for (OffsetNumber i = 0; i < chunk->chunkItemsCount; i++)
	{
		chunk->chunkItems.tupleItems[i] = itemsShift;
		ltc_set_item_flags(chunk, i, chunkBuilder->chunkItems[i].flags);

		itemsShift += chunkBuilder->chunkItems[i].size;
	}
}

/*
 * Implementation of internal tuple chunks.
 */

/*
 * Estimate size shift of the operation over the internal tuple chunk.
 */
static int32
itc_estimate_change(BTreeChunkDesc *chunk, OffsetNumber itemOffset,
					BTreeChunkOperationType operation, OTuple tuple)
{
	uint16		tupleSize = 0;
	int32		itemsShift = 0,
				sizeNeeded = 0;

	if (operation == BTreeChunkOperationInsert ||
		operation == BTreeChunkOperationUpdate)
	{
		if (chunk->chunkOffset != 0 && itemOffset != 0)
			tupleSize = o_btree_len(chunk->treeDesc, tuple,
									chunk->ops->itemLengthType);
		else
		{
			/*
			 * The leftmost tuple of the leftmost chunk doesn't store any
			 * data, therefore don't expect any passed data here.
			 */
			Assert(tuple.data == NULL);
		}
	}

	if (operation == BTreeChunkOperationInsert)
	{
		/* Calculate the change of (maxaligned) item array size */
		itemsShift = MAXALIGN(sizeof(LocationIndex) * (chunk->chunkItemsCount + 1)) -
			MAXALIGN(sizeof(LocationIndex) * chunk->chunkItemsCount);

		sizeNeeded = chunk->ops->itemHeaderSize + MAXALIGN(tupleSize) +
			itemsShift;
	}
	else if (operation == BTreeChunkOperationUpdate)
	{
		sizeNeeded = (chunk->ops->itemHeaderSize + MAXALIGN(tupleSize)) -
			ltc_get_item_size(chunk, itemOffset);

		/* We don't move items to fill the gap if the new tuple is smaller */
		if (sizeNeeded < 0)
			sizeNeeded = 0;
	}
	else if (operation == BTreeChunkOperationDelete)
	{
		/* Calculate the change of (maxaligned) item array size */
		itemsShift = MAXALIGN(sizeof(LocationIndex) * chunk->chunkItemsCount) -
			MAXALIGN(sizeof(LocationIndex) * (chunk->chunkItemsCount - 1));

		sizeNeeded = (-1) * (ltc_get_item_size(chunk, itemOffset) +
							 itemsShift);
	}
	else
		Assert(false);

	return sizeNeeded;
}

/*
 * Perform the operation over the internal tuple chunk.  A caller is responsible
 * that the chunk has enough space.
 */
static void
itc_perform_change(BTreeChunkDesc *chunk, OffsetNumber itemOffset,
				   BTreeChunkOperationType operation,
				   Pointer tupleHeader, OTuple tuple)
{
	Pointer		itemPtr;
	uint16		tupleSize = 0;

	if (operation == BTreeChunkOperationInsert ||
		operation == BTreeChunkOperationUpdate)
	{
		if (chunk->chunkOffset != 0 && itemOffset != 0)
			tupleSize = o_btree_len(chunk->treeDesc, tuple,
									chunk->ops->itemLengthType);
		else
		{
			/*
			 * The leftmost tuple of the leftmost chunk doesn't store any
			 * data, therefore don't expect any passed data here.
			 */
			Assert(tuple.data == NULL);
		}
	}

	if (operation == BTreeChunkOperationInsert)
	{
		/* Allocate space for the new item, move other items if necessary */
		ltc_allocate_item(chunk, itemOffset,
						  chunk->ops->itemHeaderSize + MAXALIGN(tupleSize));

		/* Copy the new tuple and its header */
		itemPtr = ltc_get_item(chunk, itemOffset);

		memcpy(itemPtr, tupleHeader, chunk->ops->itemHeaderSize);
		if (tupleSize > 0)
		{
			itemPtr += chunk->ops->itemHeaderSize;
			memcpy(itemPtr, tuple.data, tupleSize);
		}

		ltc_set_item_flags(chunk, itemOffset, tuple.formatFlags);
	}
	else if (operation == BTreeChunkOperationUpdate)
	{
		ltc_resize_item(chunk, itemOffset,
						chunk->ops->itemHeaderSize + MAXALIGN(tupleSize));

		/* Copy the new tuple and its header */
		itemPtr = ltc_get_item(chunk, itemOffset);

		memcpy(itemPtr, tupleHeader, chunk->ops->itemHeaderSize);
		if (tupleSize > 0)
		{
			itemPtr += chunk->ops->itemHeaderSize;
			memcpy(itemPtr, tuple.data, tupleSize);
		}

		ltc_set_item_flags(chunk, itemOffset, tuple.formatFlags);
	}
	else if (operation == BTreeChunkOperationDelete)
	{
		Assert(tupleHeader == NULL);

		ltc_delete_item(chunk, itemOffset);
	}
	else
		elog(ERROR, "invalid BTreeChunkOperationType: %d", operation);
}

const BTreeChunkOps BTreeLeafTupleChunkOps = {
	.itemHeaderSize = BTreeLeafTuphdrSize,
	.itemKeyType = BTreeKeyLeafTuple,
	.itemLengthType = OTupleLength,
	/* Main functions */
	.init = ltc_init,
	.estimate_change = ltc_estimate_change,
	.perform_change = ltc_perform_change,
	.compact = ltc_compact,
	.get_available_size = ltc_get_available_size,
	.cmp = ltc_cmp,
	.search = ltc_search,
	.read_tuple = ltc_read_tuple,
	/* Builder functions */
	.builder_init = ltc_builder_init,
	.builder_estimate = ltc_builder_estimate,
	.builder_add = ltc_builder_add,
	.builder_finish = ltc_builder_finish,
};

const BTreeChunkOps BTreeInternalTupleChunkOps = {
	.itemHeaderSize = BTreeNonLeafTuphdrSize,
	.itemKeyType = BTreeKeyNonLeafKey,
	.itemLengthType = OKeyLength,
	/* Main functions */
	.init = ltc_init,
	.estimate_change = itc_estimate_change,
	.perform_change = itc_perform_change,
	.compact = NULL,
	.get_available_size = NULL,
	.cmp = ltc_cmp,
	.search = ltc_search,
	.read_tuple = ltc_read_tuple,
	/* Builder functions */
	.builder_init = ltc_builder_init,
	.builder_estimate = ltc_builder_estimate,
	.builder_add = ltc_builder_add,
	.builder_finish = ltc_builder_finish,
};
