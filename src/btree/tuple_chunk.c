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
#include "btree/tuple_chunk.h"

static void allocate_chunk_item(BTreeChunkDesc *chunk, OffsetNumber itemOffset,
								uint16 itemSize);
static void resize_chunk_item(BTreeChunkDesc *chunk, OffsetNumber itemOffset,
							  uint16 newItemSize);
static void delete_chunk_item(BTreeChunkDesc *chunk, OffsetNumber itemOffset);

static bool load_chunk_partially(BTreeChunkDesc *chunk, PartialPageState *partial,
								 Page page);

static uint16 get_tuple_chunk_header_size(BTreeChunkType chunkType);
static uint16 get_tuple_chunk_tuple_size(BTreeDescr *treeDesc,
										 BTreeChunkType chunkType, OTuple tuple);

static void add_builder_item(BTreeChunkBuilderData *chunkBuilder,
							 Pointer tupleHeader, uint16 tupleHeaderSize,
							 OTuple tuple, uint16 tupleSize);

int32
tuple_chunk_estimate_change(BTreeChunkDesc *chunk, OffsetNumber itemOffset,
							BTreeChunkOperationType operation, OTuple tuple)
{
	int32		itemsShift = 0,
				sizeNeeded = 0;

	if (operation == BTreeChunkOperationInsert)
	{
		/* Calculate the change of (maxaligned) item array size */
		itemsShift = MAXALIGN(sizeof(LocationIndex) * (chunk->chunkItemsCount + 1)) -
			MAXALIGN(sizeof(LocationIndex) * chunk->chunkItemsCount);

		sizeNeeded = get_tuple_chunk_header_size(chunk->chunkType) +
			MAXALIGN(get_tuple_chunk_tuple_size(chunk->treeDesc,
												chunk->chunkType, tuple)) +
			itemsShift;
	}
	else if (operation == BTreeChunkOperationUpdate)
	{
		sizeNeeded = (get_tuple_chunk_header_size(chunk->chunkType) +
					  MAXALIGN(get_tuple_chunk_tuple_size(chunk->treeDesc,
														  chunk->chunkType, tuple))) -
			get_tuple_chunk_item_size(chunk, itemOffset);

		/* We don't move items to fill the gap if the new tuple is smaller */
		if (sizeNeeded < 0)
			sizeNeeded = 0;
	}
	else if (operation == BTreeChunkOperationDelete)
	{
		/* Calculate the change of (maxaligned) item array size */
		itemsShift = MAXALIGN(sizeof(LocationIndex) * chunk->chunkItemsCount) -
			MAXALIGN(sizeof(LocationIndex) * (chunk->chunkItemsCount - 1));

		sizeNeeded = (-1) * (get_tuple_chunk_item_size(chunk, itemOffset) +
							 itemsShift);
	}
	else
		Assert(false);

	return sizeNeeded;
}

void
tuple_chunk_perform_change(BTreeChunkDesc *chunk, OffsetNumber itemOffset,
						   BTreeChunkOperationType operation,
						   Pointer tupleHeader, OTuple tuple)
{
	Pointer		itemPtr;
	uint16		tupleHeaderSize,
				tupleSize;

	if (operation == BTreeChunkOperationInsert ||
		operation == BTreeChunkOperationUpdate)
	{
		tupleHeaderSize = get_tuple_chunk_header_size(chunk->chunkType);
		tupleSize = get_tuple_chunk_tuple_size(chunk->treeDesc,
											   chunk->chunkType, tuple);
	}

	if (operation == BTreeChunkOperationInsert)
	{
		/* Allocate space for the new item, move other items if necessary */
		allocate_chunk_item(chunk, itemOffset,
							tupleHeaderSize + MAXALIGN(tupleSize));

		/* Copy the new tuple and its header */
		itemPtr = get_tuple_chunk_item(chunk, itemOffset);

		memcpy(itemPtr, tupleHeader, tupleHeaderSize);
		itemPtr += tupleHeaderSize;
		memcpy(itemPtr, tuple.data, tupleSize);

		set_tuple_chunk_item_flags(chunk, itemOffset, tuple.formatFlags);
	}
	else if (operation == BTreeChunkOperationUpdate)
	{
		resize_chunk_item(chunk, itemOffset,
						  tupleHeaderSize + MAXALIGN(tupleSize));

		/* Copy the new tuple and its header */
		itemPtr = get_tuple_chunk_item(chunk, itemOffset);

		memcpy(itemPtr, tupleHeader, tupleHeaderSize);
		itemPtr += tupleHeaderSize;
		memcpy(itemPtr, tuple.data, tupleSize);

		set_tuple_chunk_item_flags(chunk, itemOffset, tuple.formatFlags);
	}
	else if (operation == BTreeChunkOperationDelete)
	{
		delete_chunk_item(chunk, itemOffset);
	}
	else
		elog(ERROR, "invalid BTreeChunkOperationType: %d", operation);
}

void
tuple_chunk_compact(BTreeChunkDesc *chunk)
{
	uint16		itemsSize;
	Pointer		batchDst,
				batchSrc;
	OffsetNumber batchOffset = 0,
				lastOffset = 0;
	uint16		batchSize = 0;

	Assert(chunk->chunkType == BTreeChunkLeaf);

	if (chunk->chunkItemsCount == 0)
		return;

	/* Detect deleted items and delete them from the items array */
	for (OffsetNumber itemOffset = 0; itemOffset < chunk->chunkItemsCount; itemOffset++)
	{
		BTreeLeafTuphdr *tupleHeader;
		OTuple		tuple;

		read_tuple_chunk_leaf_item(chunk, itemOffset, &tupleHeader, &tuple);
		if (tupleHeader->deleted == BTreeLeafTupleNonDeleted)
			chunk->chunkItems->items[lastOffset++] = chunk->chunkItems->items[itemOffset];
	}
	chunk->chunkItemsCount = lastOffset;

	/* All items were deleted, just exit */
	if (chunk->chunkItemsCount == 0)
	{
		chunk->chunkDataSize = 0;
		return;
	}

	itemsSize = MAXALIGN(sizeof(LocationIndex) * chunk->chunkItemsCount);
	batchDst = chunk->chunkData + itemsSize;

	/* Move items in batches in order to reduce number of memmove() calls */
	for (OffsetNumber itemOffset = 0; itemOffset < chunk->chunkItemsCount; itemOffset++)
	{
		OTuple		tuple;

		/*
		 * If there is a gap between the batch and the current item then move
		 * the batch and start new batch from the current item.
		 */
		if (itemOffset > 0 && get_tuple_chunk_item(chunk, itemOffset) >
			(batchDst + batchSize))
		{
			batchSrc = get_tuple_chunk_item(chunk, batchOffset);
			/* Do we actually need to move the batch? */
			if (batchDst != batchSrc)
			{
				uint16		dataShift = batchSrc - batchDst;

				memmove(batchDst, batchSrc, batchSize);

				/* Adjust the items array */
				for (OffsetNumber i = batchOffset; i < itemOffset; i++)
					chunk->chunkItems->items[i] -= dataShift;
			}

			/* Update next batch offsets */
			batchDst += batchSize;
			batchOffset = itemOffset;
			batchSize = 0;
		}

		tuple = read_tuple_chunk_tuple(chunk, itemOffset);
		batchSize += get_tuple_chunk_header_size(chunk->chunkType) +
			MAXALIGN(get_tuple_chunk_tuple_size(chunk->treeDesc,
												chunk->chunkType, tuple));
	}

	/* Move the last batch */
	batchSrc = get_tuple_chunk_item(chunk, batchOffset);
	if (batchDst != batchSrc)
	{
		uint16		dataShift = batchSrc - batchDst;

		memmove(batchDst, batchSrc, batchSize);

		/* Adjust the items array */
		for (OffsetNumber i = batchOffset; i < chunk->chunkItemsCount; i++)
			chunk->chunkItems->items[i] -= dataShift;

		chunk->chunkDataSize -= dataShift;
	}
}

bool
tuple_chunk_cmp(BTreeChunkDesc *chunk, PartialPageState *partial,
				Page page, OffsetNumber itemOffset,
				void *key, BTreeKeyType keyType, int *result)
{
	OTuple		tuple2;
	BTreeKeyType keyType2;
	OBTreeKeyCmp cmpFunc = chunk->treeDesc->ops->cmp;

	if (partial && !load_chunk_partially(chunk, partial, page))
		return false;

	Assert(itemOffset < chunk->chunkItemsCount);

	keyType2 = chunk->chunkType == BTreeChunkLeaf ? BTreeKeyLeafTuple : BTreeKeyNonLeafKey;
	tuple2 = read_tuple_chunk_tuple(chunk, itemOffset);

	*result = cmpFunc(chunk->treeDesc, key, keyType, &tuple2, keyType2);
	return true;
}

bool
tuple_chunk_search(BTreeChunkDesc *chunk, PartialPageState *partial,
				   Page page, void *key, BTreeKeyType keyType,
				   OffsetNumber *itemOffset)
{
	bool		isLeaf = chunk->chunkType == BTreeChunkLeaf,
				nextKey;
	BTreeKeyType midKeyType;
	OffsetNumber low,
				mid,
				high;
	OBTreeKeyCmp cmpFunc = chunk->treeDesc->ops->cmp;
	int			targetCmpVal,
				result;

	if (partial && !load_chunk_partially(chunk, partial, page))
		return false;

	if (chunk->chunkItemsCount == 0)
	{
		*itemOffset = 0;
		return true;
	}

	midKeyType = isLeaf ? BTreeKeyLeafTuple : BTreeKeyNonLeafKey;

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
			OTuple		midTuple;

			*itemOffset = mid;
			midTuple = read_tuple_chunk_tuple(chunk, *itemOffset);
			result = cmpFunc(chunk->treeDesc, key, keyType, &midTuple, midKeyType);
		}

		if (result >= targetCmpVal)
			low = mid + 1;
		else
			high = mid;
	}

	*itemOffset = low;

	return true;
}

bool
tuple_chunk_read_tuple(BTreeChunkDesc *chunk, PartialPageState *partial,
					   Page page, OffsetNumber itemOffset,
					   Pointer *tupleHeader, OTuple *tuple, bool *needsFree)
{
	if (partial && !load_chunk_partially(chunk, partial, page))
		return false;

	Assert(itemOffset < chunk->chunkItemsCount);

	if (chunk->chunkType == BTreeChunkLeaf)
		read_tuple_chunk_leaf_item(chunk, itemOffset,
								   (BTreeLeafTuphdr **) tupleHeader, tuple);
	else
		read_tuple_chunk_internal_item(chunk, itemOffset,
									   (BTreeNonLeafTuphdr **) tupleHeader,
									   tuple);

	/* Set always to true in the current implementation */
	*needsFree = true;

	return true;
}

void
init_tuple_chunk(BTreeChunkDesc *chunk, Page page, OffsetNumber chunkOffset)
{
	BTreePageHeader *header = (BTreePageHeader *) page;
	LocationIndex chunkLocation = SHORT_GET_LOCATION(header->chunkDesc[chunkOffset].shortLocation);

	chunk->chunkOffset = chunkOffset;
	chunk->chunkData = (Pointer) page + chunkLocation;
	chunk->chunkItems = (BTreePageChunk *) (page + chunkLocation);

	if (chunkOffset + 1 < header->chunksCount)
	{
		chunk->chunkItemsCount = header->chunkDesc[chunkOffset + 1].offset -
			header->chunkDesc[chunkOffset].offset;
		chunk->chunkDataSize = SHORT_GET_LOCATION(header->chunkDesc[chunkOffset + 1].shortLocation) -
			chunkLocation;
	}
	else
	{
		chunk->chunkItemsCount = header->itemsCount -
			header->chunkDesc[chunkOffset].offset;
		chunk->chunkDataSize = header->dataSize - chunkLocation;
	}
}

static void
allocate_chunk_item(BTreeChunkDesc *chunk, OffsetNumber itemOffset,
					uint16 itemSize)
{
	LocationIndex itemsShift,
				dataShift;
	Pointer		firstItemPtr,
				itemPtr,
				endPtr;

	Assert(itemSize == MAXALIGN(itemSize));

	/* Calculate the change of (maxaligned) item array size */
	itemsShift = MAXALIGN(sizeof(LocationIndex) * (chunk->chunkItemsCount + 1)) -
		MAXALIGN(sizeof(LocationIndex) * chunk->chunkItemsCount);

	/* Calculate the shift of the data after new item is inserted */
	dataShift = itemsShift + itemSize;
	Assert(dataShift == MAXALIGN(dataShift));

	if (chunk->chunkItemsCount > 0)
		firstItemPtr = get_tuple_chunk_item(chunk, 0);
	else
		firstItemPtr = chunk->chunkData +
			MAXALIGN(sizeof(LocationIndex) * chunk->chunkItemsCount);

	if (itemOffset < chunk->chunkItemsCount)
		itemPtr = get_tuple_chunk_item(chunk, itemOffset);
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

	if (itemsShift != 0)
	{
		/*
		 * If items array size is changed, then we have to also move the items
		 * before insert location and adjust those locations in the items
		 * array.
		 */
		memmove(firstItemPtr + itemsShift, firstItemPtr, itemPtr - firstItemPtr);
		for (OffsetNumber i = 0; i < itemOffset; i++)
			chunk->chunkItems->items[i] += itemsShift;
	}

	/* Add the new item to the items array  */
	for (OffsetNumber i = chunk->chunkItemsCount; i > itemOffset; i--)
		chunk->chunkItems->items[i] = chunk->chunkItems->items[i - 1] + dataShift;
	chunk->chunkItems->items[itemOffset] = itemPtr - chunk->chunkData + itemsShift;
}

static void
resize_chunk_item(BTreeChunkDesc *chunk, OffsetNumber itemOffset,
				  uint16 newItemSize)
{
	int32		dataShift;
	Pointer		nextItemPtr,
				endPtr;

	Assert(newItemSize == MAXALIGN(newItemSize));

	/* Calculate the shift of the data after the item resize */
	dataShift = newItemSize - get_tuple_chunk_item_size(chunk, itemOffset);
	Assert(dataShift == MAXALIGN(dataShift));

	/* We don't move items to fill the gap if the new tuple is smaller */
	if (dataShift <= 0)
		return;

	Assert(itemOffset < chunk->chunkItemsCount);
	if (itemOffset + 1 < chunk->chunkItemsCount)
		nextItemPtr = get_tuple_chunk_item(chunk, itemOffset + 1);
	else
		nextItemPtr = chunk->chunkData + chunk->chunkDataSize;

	endPtr = chunk->chunkData + chunk->chunkDataSize;

	/* Data should still fit to the page */
	Assert(chunk->chunkDataSize + dataShift <= ORIOLEDB_BLCKSZ);

	/* Shift the data after the item */
	memmove(nextItemPtr + dataShift, nextItemPtr, endPtr - nextItemPtr);

	/* Adjust the items array */
	for (OffsetNumber i = itemOffset + 1; i < chunk->chunkItemsCount; i++)
		chunk->chunkItems->items[i] += dataShift;

	chunk->chunkDataSize += dataShift;
}

static void
delete_chunk_item(BTreeChunkDesc *chunk, OffsetNumber itemOffset)
{
	uint16		itemSize;
	LocationIndex itemsShift,
				dataShift;
	Pointer		firstItemPtr,
				itemPtr,
				endPtr;

	itemSize = get_tuple_chunk_item_size(chunk, itemOffset);
	Assert(itemSize == MAXALIGN(itemSize));

	Assert(chunk->chunkItemsCount > 0);

	/* Calculate the change of (maxaligned) item array size */
	itemsShift = MAXALIGN(sizeof(LocationIndex) * chunk->chunkItemsCount) -
		MAXALIGN(sizeof(LocationIndex) * (chunk->chunkItemsCount - 1));

	/* Calculate the shift of the data after the item is deleted */
	dataShift = itemsShift + itemSize;
	Assert(dataShift == MAXALIGN(dataShift));

	firstItemPtr = get_tuple_chunk_item(chunk, 0);

	Assert(itemOffset < chunk->chunkItemsCount);
	itemPtr = get_tuple_chunk_item(chunk, itemOffset);

	endPtr = chunk->chunkData + chunk->chunkDataSize;
	Assert(endPtr - dataShift >= itemPtr - itemsShift);

	/*
	 * Adjust the items array.  We should do this first to prevent it been
	 * overridden by the data when it's shorten.
	 */
	for (OffsetNumber i = itemOffset; i < chunk->chunkItemsCount - 1; i++)
		chunk->chunkItems->items[i] = chunk->chunkItems->items[i + 1] - dataShift;

	if (itemsShift != 0)
	{
		/* Shift the data before deleted item when items arrays is shorten. */
		memmove(firstItemPtr - itemsShift, firstItemPtr, itemPtr - firstItemPtr);

		/* Shift item pointers of those items */
		for (OffsetNumber i = 0; i < itemOffset; i++)
			chunk->chunkItems->items[i] -= itemsShift;
	}

	/* Move the data after deleted item */
	memmove(itemPtr - itemsShift, itemPtr + itemSize, endPtr - itemPtr - itemSize);

	chunk->chunkItemsCount--;
	chunk->chunkDataSize -= dataShift;
}

static bool
load_chunk_partially(BTreeChunkDesc *chunk, PartialPageState *partial, Page page)
{
	Page		sourcePage = partial->src;
	uint32		pageState,
				sourceState;
	BTreePageHeader *header = (BTreePageHeader *) page;
	LocationIndex chunkLocation =
		SHORT_GET_LOCATION(header->chunkDesc[chunk->chunkOffset].shortLocation);

	Assert(partial != NULL);

	if (!partial->isPartial || partial->chunkIsLoaded[chunk->chunkOffset])
		return true;

	init_tuple_chunk(chunk, page, chunk->chunkOffset);

	memcpy((Pointer) page + chunkLocation,
		   (Pointer) sourcePage + chunkLocation, chunk->chunkDataSize);

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

static inline uint16
get_tuple_chunk_header_size(BTreeChunkType chunkType)
{
	if (chunkType == BTreeChunkLeaf)
		return sizeof(BTreeLeafTuphdr);
	else if (chunkType == BTreeChunkNonLeaf ||
			 chunkType == BTreeChunkHiKeys)
		return sizeof(BTreeNonLeafTuphdr);
	else
		Assert(false);
	/* Make compiler quite */
	return 0;
}

static inline uint16
get_tuple_chunk_tuple_size(BTreeDescr *treeDesc, BTreeChunkType chunkType,
						   OTuple tuple)
{
	if (chunkType == BTreeChunkLeaf)
		return o_btree_len(treeDesc, tuple, OTupleLength);
	else if (chunkType == BTreeChunkNonLeaf ||
			 chunkType == BTreeChunkHiKeys)
		return o_btree_len(treeDesc, tuple, OKeyLength);
	else
		Assert(false);
	/* Make compiler quite */
	return 0;
}

void
tuple_chunk_builder_init(BTreeChunkBuilderData *chunkBuilder,
						 BTreeDescr *treeDesc, BTreeChunkType chunkType)
{
	chunkBuilder->treeDesc = treeDesc;
	chunkBuilder->chunkType = chunkType;

	chunkBuilder->chunkData = NULL;
	chunkBuilder->chunkDataMaxSize = 0;
	chunkBuilder->chunkDataSize = 0;

	chunkBuilder->chunkItemsCount = 0;
}

int32
tuple_chunk_builder_estimate(BTreeChunkBuilderData *chunkBuilder,
							 OTuple tuple)
{
	LocationIndex itemsShift;

	Assert(chunkBuilder != NULL);

	itemsShift = MAXALIGN(sizeof(LocationIndex) * (chunkBuilder->chunkItemsCount + 1)) -
		MAXALIGN(sizeof(LocationIndex) * chunkBuilder->chunkItemsCount);

	return itemsShift + MAXALIGN(
								 get_tuple_chunk_tuple_size(chunkBuilder->treeDesc,
															chunkBuilder->chunkType, tuple)) +
		get_tuple_chunk_header_size(chunkBuilder->chunkType);
}

void
tuple_chunk_builder_add(BTreeChunkBuilderData *chunkBuilder, OTuple tuple,
						uint64 downlink)
{
	Size		tupleHeaderSize,
				tupleSize;

	Assert(chunkBuilder != NULL);
	Assert(chunkBuilder->chunkDataMaxSize > 0);
	Assert(chunkBuilder->chunkDataSize < chunkBuilder->chunkDataMaxSize);

	tupleHeaderSize = get_tuple_chunk_header_size(chunkBuilder->chunkType);
	tupleSize = get_tuple_chunk_tuple_size(chunkBuilder->treeDesc,
										   chunkBuilder->chunkType, tuple);

	if (chunkBuilder->chunkType == BTreeChunkLeaf)
	{
		BTreeLeafTuphdr leaf_header = {0};

		leaf_header.deleted = BTreeLeafTupleNonDeleted;
		leaf_header.undoLocation = InvalidUndoLocation;
		leaf_header.xactInfo = OXID_GET_XACT_INFO(BootstrapTransactionId, RowLockUpdate, false);

		add_builder_item(chunkBuilder, (Pointer) &leaf_header, tupleHeaderSize,
						 tuple, tupleSize);
	}
	else if (chunkBuilder->chunkType == BTreeChunkNonLeaf ||
			 chunkBuilder->chunkType == BTreeChunkHiKeys)
	{
		BTreeNonLeafTuphdr internal_header = {0};

		internal_header.downlink = downlink;

		add_builder_item(chunkBuilder, (Pointer) &internal_header, tupleHeaderSize,
						 tuple, tupleSize);
	}
	else
		Assert(false);
}

void
tuple_chunk_builder_finish(BTreeChunkBuilderData *chunkBuilder,
						   BTreeChunkDesc *chunk)
{
	uint16		chunkItemsSize;
	Pointer		itemPtr;

	chunk->treeDesc = chunkBuilder->treeDesc;
	chunk->chunkType = chunkBuilder->chunkType;

	chunk->chunkData = chunkBuilder->chunkData;
	chunk->chunkDataSize = chunkBuilder->chunkDataSize;

	chunk->chunkItemsCount = chunkBuilder->chunkItemsCount;

	/* Move items and put chunkItems into the beginning of chunkData */

	chunkItemsSize = MAXALIGN(sizeof(LocationIndex) * chunk->chunkItemsCount);
	itemPtr = chunk->chunkData + chunkItemsSize;

	memmove(itemPtr, chunk->chunkData, chunk->chunkDataSize);

	memcpy(chunk->chunkData, chunkBuilder->chunkItems,
		   sizeof(LocationIndex) * chunkBuilder->chunkItemsCount);
	chunk->chunkItems = (BTreePageChunk *) chunk->chunkData;

	/* Adjust items offets */
	for (OffsetNumber i = 0; i < chunk->chunkItemsCount; i++)
		chunk->chunkItems->items[i] += chunkItemsSize;
}

static void
add_builder_item(BTreeChunkBuilderData *chunkBuilder,
				 Pointer tupleHeader, uint16 tupleHeaderSize,
				 OTuple tuple, uint16 tupleSize)
{
	Pointer		itemPtr;
	uint16		itemSize = tupleHeaderSize + MAXALIGN(tupleSize);

	Assert(chunkBuilder->chunkDataMaxSize >= chunkBuilder->chunkDataSize + itemSize);

	itemPtr = chunkBuilder->chunkData + chunkBuilder->chunkDataSize;

	/* Copy the header and the tuple */
	memcpy(itemPtr, tupleHeader, tupleHeaderSize);
	itemPtr += tupleHeaderSize;
	memcpy(itemPtr, tuple.data, tupleSize);

	/* Adjust the item offset */
	chunkBuilder->chunkItems->items[chunkBuilder->chunkItemsCount] =
		chunkBuilder->chunkDataSize;
	chunkBuilder->chunkItems->items[chunkBuilder->chunkItemsCount] =
		ITEM_SET_FLAGS(chunkBuilder->chunkItems->items[chunkBuilder->chunkItemsCount],
					   tuple.formatFlags);

	/* Ajust the size */
	chunkBuilder->chunkDataSize = chunkBuilder->chunkDataSize + itemSize;
	chunkBuilder->chunkItemsCount++;
}
